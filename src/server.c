#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>

#define PORT 9090
#define BACKLOG 10

long get_file_size(const char *filename) 
{
    struct stat st;

    if (stat(filename, &st) != 0) 
    {
        perror("stat failed");
        return -1;
    }

    return st.st_size;
}

int main(int argc, char *argv[]) 
{
    if (argc != 2) 
    {
        fprintf(stderr, "Usage: %s <input_file>\n", argv[0]);
        return 1;
    }

    const char *input_file = argv[1];

    long file_size = get_file_size(input_file);

    if (file_size <= 0) 
    {
        fprintf(stderr, "Invalid file size or file not found.\n");
        return 1;
    }

    printf("Input file: %s\n", input_file);
    printf("File size: %ld bytes\n", file_size);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) 
    {
        perror("socket failed");
        return 1;
    }

    int opt = 1;

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) 
    {
        perror("setsockopt failed");
        close(server_fd);
        return 1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) 
    {
        perror("bind failed");
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, BACKLOG) < 0) 
    {
        perror("listen failed");
        close(server_fd);
        return 1;
    }

    printf("Server listening on port %d...\n", PORT);
    printf("Waiting for client connections...\n");

    while (1) 
    {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);

        if (client_fd < 0) 
        {
            perror("accept failed");
            continue;
        }

        printf("Client connected.\n");

        pid_t pid = fork();

        if  (pid < 0)
        {
            perror("fork failed");
            close(client_fd);
            continue;
        }

        if (pid == 0)
        {
            //child process handles the client
            close(server_fd);

            printf("Child process created to handle client.\n");

            uint32_t request[2];

            ssize_t received = recv(client_fd, request, sizeof(request), MSG_WAITALL);

            if (received != sizeof(request)) 
            {
                fprintf(stderr, "Failed to receive chunk request\n");
                close(client_fd);
                exit(1);
            }

            uint32_t chunk_no = ntohl(request[0]);
            uint32_t total_chunks = ntohl(request[1]);

            printf("Client requested chunk %u of %u\n", chunk_no, total_chunks);

            close(client_fd);
            exit(0);
        }
        else
        {
            //Parent process closes client socket and continues accepting
            close(client_fd);

            while (waitpid(-1, NULL, WNOHANG) > 0)
                {
                    //Clean up finished child process
                }
        }
    }
    
    close(server_fd);
    return 0;
}
