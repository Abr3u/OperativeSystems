#include "kos_client.h"
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include "hashlists.h"
#include "server.h"

int kos_init(int num_server_threads, int buf_size, int num_shards){
	database = (hashtable**)malloc(num_shards*sizeof(hashtable*));
	buf = (buffer*)malloc(buf_size*sizeof(buffer));
	servers = (pthread_t*)malloc(num_server_threads*sizeof(pthread_t));
	shardbuf = (shardbuffer*)malloc(num_shards*sizeof(shardbuffer));
	int i;
	
	shards = num_shards;
	bufs = buf_size;
	for (i=0;i<num_shards;i++){
		database[i] = hash_table_new();
		sem_init(&shardbuf[i].leitores, 0, 0);
		sem_init(&shardbuf[i].escritores, 0, 0);
		shardbuf[i].nleitores = 0;
		shardbuf[i].escrevendo=FALSE;
		shardbuf[i].leitores_espera=0;
		shardbuf[i].escritores_espera=0;
		}
	
	for(i = 0; i<buf_size; i++){
		sem_init(&buf[i].pode_resposta, 0, 0);
		buf[i].feito = 1;
	}
	
	sem_init(&pode_server, 0, 0);
	sem_init(&pode_client, 0, buf_size);
	
	for (i=0;i<num_server_threads;i++){
		if(pthread_create(&servers[i],NULL,&server_thread, NULL)!=0)
			return -1;
	}
	
	for(i=0;i<num_shards;i++){
		reinicia(database[i], i);
		}
	return 0;

}

char* kos_get(int clientid, int shardId, char* key) {
	sem_wait(&pode_client);
	char* res = (char*)malloc(sizeof(char)*KV_SIZE);
	buffer* aux;
	int aux2 = clientid%bufs;
	pthread_mutex_lock(&Mutex_c);
	while(1){
		aux = &buf[aux2];
		if(aux->feito == 0){
			aux2 = clientid+1;
			aux2 = aux2%bufs;
			}
		else
			break;
	}
	aux->shardID = shardId;
	aux->key = key;
	aux->funcao = 0;
	aux->feito = 0;
	sem_post(&pode_server);
	sem_wait(&aux->pode_resposta);
	if (aux->value == NULL){
		aux->feito = 1;
		sem_post(&pode_client);
		pthread_mutex_unlock(&Mutex_c);
		return NULL;
	}
	else{
		strcpy(res, aux->value);
		aux->feito = 1;
		sem_post(&pode_client);
		pthread_mutex_unlock(&Mutex_c);
		return res; //retorna o value anterior associado a esta key
	}
}

char* kos_put(int clientid, int shardId, char* key, char* value) {
	sem_wait(&pode_client);
	char* res = (char*)malloc(sizeof(char)*KV_SIZE);
	buffer* aux;
	int aux2 = clientid%bufs;
	pthread_mutex_lock(&Mutex_c);
	while(1){
		aux = &buf[aux2];
		if(aux->feito == 0){
			aux2 = clientid+1;
			aux2 = aux2%bufs;
			}
		else
			break;
	}
	aux->shardID= shardId;
	aux->key = key;
	aux->value = value;
	aux->funcao = 1;
	aux->feito = 0;
	sem_post(&pode_server);
	sem_wait(&aux->pode_resposta);
	if (aux->value == NULL){
		aux->feito = 1;
		sem_post(&pode_client);
		pthread_mutex_unlock(&Mutex_c);
		return NULL;
	}
	else{
		strcpy(res, aux->value);
		aux->feito = 1;
		sem_post(&pode_client);
		pthread_mutex_unlock(&Mutex_c);
		return res; //retorna o value anterior associado a esta key
	}
}

char* kos_remove(int clientid, int shardId, char* key) {
	sem_wait(&pode_client);
	char* res = (char*)malloc(sizeof(char)*KV_SIZE);
	buffer* aux;
	int aux2 = clientid%bufs;
	pthread_mutex_lock(&Mutex_c);
	while(1){
		aux = &buf[aux2];
		if(aux->feito == 0){
			aux2 = clientid+1;
			aux2 = aux2%bufs;
			}
		else
			break;
	}
	aux->shardID= shardId;
	aux->key = key;
	aux->funcao = 2;
	aux->feito = 0;
	sem_post(&pode_server);
	sem_wait(&aux->pode_resposta);
	if (aux->value == NULL){
		aux->feito = 1;
		sem_post(&pode_client);
		pthread_mutex_unlock(&Mutex_c);
		return NULL;
	}
	else{
		strcpy(res, aux->value);
		aux->feito = 1;
		sem_post(&pode_client);
		pthread_mutex_unlock(&Mutex_c);
		return res; //retorna o value anterior associado a esta key
	}
}

KV_t* kos_getAllKeys(int clientid, int shardId, int* dim) {
	sem_wait(&pode_client);
	KV_t* res;
	buffer* aux;
	int aux2 = clientid%bufs;
	pthread_mutex_lock(&Mutex_c);
	while(1){
		aux = &buf[aux2];
		if(aux->feito == 0){
			aux2 = clientid+1;
			aux2 = aux2%bufs;
			}
		else
			break;
	}
	aux->shardID = shardId;
	aux->funcao = 3;
	aux->feito = 0;
	sem_post(&pode_server);
	sem_wait(&aux->pode_resposta);
	if (aux->listaKey == NULL){
		aux->feito = 1;
		*dim = 0;
		sem_post(&pode_client);
		pthread_mutex_unlock(&Mutex_c);
		return NULL;
	}
	else{
		*dim = conta_shard(shardId);
		res = (KV_t*)malloc(sizeof(KV_t)*(*dim));
		memcpy(res, aux->listaKey, (*dim)*sizeof(KV_t));
		aux->feito = 1;
		sem_post(&pode_client);
		pthread_mutex_unlock(&Mutex_c);
		return res;
	}
}

