#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <semaphore.h>
#include "kos_client.h"

typedef struct{
	sem_t pode_resposta;
	int feito;
	int shardID,funcao;
	int* dim;
	KV_t* listaKey;
	//char key[KV_SIZE], value[KV_SIZE];
	char *key, *value;
}buffer;

sem_t pode_client;
sem_t pode_server;

pthread_mutex_t Mutex_c;
pthread_mutex_t Mutex_s;
buffer* buf;
hashtable** database;
shardbuffer *shardbuf;
pthread_t* servers;
int shards, bufs,prodptr,consptr;

void *server_thread(void *aux1);
