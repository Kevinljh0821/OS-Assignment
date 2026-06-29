#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define SERVER_PORT 9090
#define DEFAULT_SERVER_IP "127.0.0.1"
#define OUTPUT_FILE "reassembled.dat"
#define MAX_CHUNKS 4096
#define DEFAULT_THREADS 4

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    sem_t received_sem;

    int total_chunks;
    int received_count;
    int error_flag;

    size_t base_chunk_size;

    unsigned char received[MAX_CHUNKS];
    uint32_t payload_sizes[MAX_CHUNKS];
} shared_state_t;

static int send_all(int fd, const void *buffer, size_t length) {
    const char *ptr = (const char *)buffer;
    size_t sent = 0;

    while (sent < length) {
        ssize_t n = send(fd, ptr + sent, length - sent, 0);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        if (n == 0) {
            return -1;
        }

        sent += (size_t)n;
    }

    return 0;
}

static int recv_all(int fd, void *buffer, size_t length) {
    char *ptr = (char *)buffer;
    size_t received = 0;

    while (received < length) {
        ssize_t n = recv(fd, ptr + received, length - received, 0);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        if (n == 0) {
            return -1;
        }

        received += (size_t)n;
    }

    return 0;
}

static int pwrite_all(int fd, const void *buffer, size_t length, off_t offset) {
    const char *ptr = (const char *)buffer;
    size_t written = 0;

    while (written < length) {
        ssize_t n = pwrite(fd, ptr + written, length - written, offset + written);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        if (n == 0) {
            return -1;
        }

        written += (size_t)n;
    }

    return 0;
}

static void set_error(shared_state_t *state) {
    pthread_mutex_lock(&state->mutex);
    state->error_flag = 1;
    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);
}

static int connect_to_server(const char *server_ip) {
    int sockfd;
    struct sockaddr_in server_addr;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);

    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid server IP address: %s\n", server_ip);
        close(sockfd);
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(sockfd);
        return -1;
    }

    return sockfd;
}

static int receive_one_chunk(
    const char *server_ip,
    int chunk_number,
    int total_chunks,
    int output_fd,
    shared_state_t *state
) {
    int sockfd = -1;

    uint32_t request[2];
    uint32_t net_seq;
    uint32_t net_payload_size;
    uint32_t sequence_number;
    uint32_t payload_size;

    char *payload = NULL;

    sockfd = connect_to_server(server_ip);

    if (sockfd < 0) {
        set_error(state);
        sem_post(&state->received_sem);
        return 1;
    }

    /*
     * Client request format:
     * [4 bytes chunk number]
     * [4 bytes total chunks]
     */
    request[0] = htonl((uint32_t)chunk_number);
    request[1] = htonl((uint32_t)total_chunks);

    if (send_all(sockfd, request, sizeof(request)) < 0) {
        perror("send request");
        close(sockfd);
        set_error(state);
        sem_post(&state->received_sem);
        return 1;
    }

    /*
     * Server response format:
     * [4 bytes sequence number]
     * [4 bytes payload size]
     * [payload data]
     */
    if (recv_all(sockfd, &net_seq, sizeof(net_seq)) < 0) {
        perror("recv sequence number");
        close(sockfd);
        set_error(state);
        sem_post(&state->received_sem);
        return 1;
    }

    if (recv_all(sockfd, &net_payload_size, sizeof(net_payload_size)) < 0) {
        perror("recv payload size");
        close(sockfd);
        set_error(state);
        sem_post(&state->received_sem);
        return 1;
    }

    sequence_number = ntohl(net_seq);
    payload_size = ntohl(net_payload_size);

    if ((int)sequence_number != chunk_number) {
        fprintf(stderr,
                "Sequence mismatch: requested chunk %d but received chunk %u\n",
                chunk_number,
                sequence_number);

        close(sockfd);
        set_error(state);
        sem_post(&state->received_sem);
        return 1;
    }

    payload = malloc(payload_size);

    if (payload == NULL) {
        perror("malloc payload");
        close(sockfd);
        set_error(state);
        sem_post(&state->received_sem);
        return 1;
    }

    if (recv_all(sockfd, payload, payload_size) < 0) {
        perror("recv payload");
        free(payload);
        close(sockfd);
        set_error(state);
        sem_post(&state->received_sem);
        return 1;
    }

    close(sockfd);

    /*
     * Synchronization section:
     * The client needs to know the normal chunk size before calculating offsets.
     * Any non-last chunk can be used as the base chunk size.
     */
    pthread_mutex_lock(&state->mutex);

    if (state->base_chunk_size == 0 &&
        (sequence_number < (uint32_t)total_chunks || total_chunks == 1)) {
        state->base_chunk_size = payload_size;
        pthread_cond_broadcast(&state->cond);
    }

    while (state->base_chunk_size == 0 && state->error_flag == 0) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }

    if (state->error_flag) {
        pthread_mutex_unlock(&state->mutex);
        free(payload);
        sem_post(&state->received_sem);
        return 1;
    }

    size_t base_chunk_size = state->base_chunk_size;

    pthread_mutex_unlock(&state->mutex);

    /*
     * Correct output position:
     * chunk 1 -> offset 0
     * chunk 2 -> offset base_chunk_size
     * chunk 3 -> offset 2 * base_chunk_size
     */
    off_t output_offset = (off_t)(sequence_number - 1) * (off_t)base_chunk_size;

    if (pwrite_all(output_fd, payload, payload_size, output_offset) < 0) {
        perror("pwrite reassembled.dat");
        free(payload);
        set_error(state);
        sem_post(&state->received_sem);
        return 1;
    }

    free(payload);

    /*
     * Protect shared received flags and counter using mutex.
     */
    pthread_mutex_lock(&state->mutex);

    if (state->received[sequence_number - 1] == 0) {
        state->received[sequence_number - 1] = 1;
        state->payload_sizes[sequence_number - 1] = payload_size;
        state->received_count++;
    }

    pthread_cond_broadcast(&state->cond);
    pthread_mutex_unlock(&state->mutex);

    sem_post(&state->received_sem);

    return 0;
}

static int initialize_shared_state(shared_state_t *state, int total_chunks) {
    pthread_mutexattr_t mutex_attr;
    pthread_condattr_t cond_attr;

    memset(state, 0, sizeof(shared_state_t));

    state->total_chunks = total_chunks;

    if (pthread_mutexattr_init(&mutex_attr) != 0) {
        perror("pthread_mutexattr_init");
        return -1;
    }

    if (pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED) != 0) {
        perror("pthread_mutexattr_setpshared");
        return -1;
    }

    if (pthread_mutex_init(&state->mutex, &mutex_attr) != 0) {
        perror("pthread_mutex_init");
        return -1;
    }

    if (pthread_condattr_init(&cond_attr) != 0) {
        perror("pthread_condattr_init");
        return -1;
    }

    if (pthread_condattr_setpshared(&cond_attr, PTHREAD_PROCESS_SHARED) != 0) {
        perror("pthread_condattr_setpshared");
        return -1;
    }

    if (pthread_cond_init(&state->cond, &cond_attr) != 0) {
        perror("pthread_cond_init");
        return -1;
    }

    if (sem_init(&state->received_sem, 1, 0) != 0) {
        perror("sem_init");
        return -1;
    }

    pthread_mutexattr_destroy(&mutex_attr);
    pthread_condattr_destroy(&cond_attr);

    return 0;
}

static int launch_operations(int thread_count) {
    pid_t pid;
    int status;
    char thread_arg[32];

    snprintf(thread_arg, sizeof(thread_arg), "%d", thread_count);

    pid = fork();

    if (pid < 0) {
        perror("fork operations");
        return -1;
    }

    if (pid == 0) {
        execl("./operations",
              "./operations",
              "-t",
              thread_arg,
              "-f",
              OUTPUT_FILE,
              (char *)NULL);

        perror("execl operations");
        _exit(127);
    }

    if (waitpid(pid, &status, 0) < 0) {
        perror("waitpid operations");
        return -1;
    }

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fprintf(stderr, "operations program failed\n");
        return -1;
    }

    return 0;
}

static void print_usage(const char *program_name) {
    fprintf(stderr,
            "Usage: %s -p <N> [-t <T>] [-h <server_ip>]\n"
            "Example: %s -p 4 -t 8\n",
            program_name,
            program_name);
}

int main(int argc, char *argv[]) {
    int opt;
    int total_chunks = 0;
    int thread_count = DEFAULT_THREADS;
    const char *server_ip = DEFAULT_SERVER_IP;

    int output_fd;
    shared_state_t *state;
    pid_t children[MAX_CHUNKS];

    while ((opt = getopt(argc, argv, "p:t:h:")) != -1) {
        switch (opt) {
            case 'p':
                total_chunks = atoi(optarg);
                break;

            case 't':
                thread_count = atoi(optarg);
                break;

            case 'h':
                server_ip = optarg;
                break;

            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    if (total_chunks <= 0 || total_chunks > MAX_CHUNKS) {
        fprintf(stderr, "Invalid number of chunks. Use 1 to %d.\n", MAX_CHUNKS);
        print_usage(argv[0]);
        return 1;
    }

    if (thread_count <= 0) {
        fprintf(stderr, "Invalid thread count.\n");
        return 1;
    }

    state = mmap(NULL,
                 sizeof(shared_state_t),
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED | MAP_ANONYMOUS,
                 -1,
                 0);

    if (state == MAP_FAILED) {
        perror("mmap shared state");
        return 1;
    }

    if (initialize_shared_state(state, total_chunks) < 0) {
        munmap(state, sizeof(shared_state_t));
        return 1;
    }

    output_fd = open(OUTPUT_FILE, O_CREAT | O_TRUNC | O_RDWR, 0644);

    if (output_fd < 0) {
        perror("open reassembled.dat");
        munmap(state, sizeof(shared_state_t));
        return 1;
    }

    /*
     * Create N child processes.
     * Each child receives exactly one chunk from the server.
     */
    for (int i = 0; i < total_chunks; i++) {
        pid_t pid = fork();

        if (pid < 0) {
            perror("fork client child");
            set_error(state);
            break;
        }

        if (pid == 0) {
            int chunk_number = i + 1;

            int result = receive_one_chunk(server_ip,
                                           chunk_number,
                                           total_chunks,
                                           output_fd,
                                           state);

            close(output_fd);
            _exit(result);
        }

        children[i] = pid;
    }

    /*
     * Semaphore waits until all child processes have completed
     * their chunk transfer or reported an error.
     */
    for (int i = 0; i < total_chunks; i++) {
        sem_wait(&state->received_sem);
    }

    int child_failed = 0;

    for (int i = 0; i < total_chunks; i++) {
        int status;

        if (waitpid(children[i], &status, 0) < 0) {
            perror("waitpid client child");
            child_failed = 1;
            continue;
        }

        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            child_failed = 1;
        }
    }

    /*
     * Verify that all chunks were received.
     */
    pthread_mutex_lock(&state->mutex);

    int all_received = 1;

    for (int i = 0; i < total_chunks; i++) {
        if (state->received[i] == 0) {
            all_received = 0;
            fprintf(stderr, "Missing chunk %d\n", i + 1);
        }
    }

    pthread_mutex_unlock(&state->mutex);

    if (state->error_flag || child_failed || !all_received) {
        fprintf(stderr, "Client failed: not all chunks were received correctly.\n");
        close(output_fd);
        munmap(state, sizeof(shared_state_t));
        return 1;
    }

    if (fsync(output_fd) < 0) {
        perror("fsync");
        close(output_fd);
        munmap(state, sizeof(shared_state_t));
        return 1;
    }

    close(output_fd);

    printf("[CLIENT] Reassembly completed successfully: %s\n", OUTPUT_FILE);
    printf("[CLIENT] Launching operations program...\n");

    if (launch_operations(thread_count) < 0) {
        fprintf(stderr, "Failed to launch operations program.\n");
        munmap(state, sizeof(shared_state_t));
        return 1;
    }

    printf("[CLIENT] Operations completed successfully.\n");

    munmap(state, sizeof(shared_state_t));

    return 0;
}
