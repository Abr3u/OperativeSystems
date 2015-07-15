#include "kos_client.h"
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include "hashlists.h"
#include "server.h"
#include "delay.h"

void *server_thread(void *aux1){
	int i=0, p=0;
	hashtable* shard;
	buffer *aux;
	char* charaux = (char*)malloc(KV_SIZE*sizeof(char));
	while(1){
		if (p==0)
			sem_wait(&pode_server);
		pthread_mutex_lock(&Mutex_s);
		i=i%bufs;
		aux = &buf[i];
		if(aux->feito == 1){
			pthread_mutex_unlock(&Mutex_s);
			p=1;
			i++;
		}
		else{
			shard = database[aux->shardID];
			if(aux->funcao == 0){
				delay();
				aux->value = hash_table_get(shard, aux->key, aux->shardID);
				}
			if(aux->funcao == 1){
				strcpy(charaux, aux->value);
				delay();
				aux->value = kv_hash_insert(shard, aux->key, aux->value, aux->shardID);
				insert_infile(shard, aux->shardID, aux->key, charaux, aux->value);
			}
			if(aux->funcao == 2){
				delay();
				aux->value = hash_table_remove(shard, aux->key, aux->shardID);
				remove_offile(shard, aux->shardID, aux->key);
			}
			if(aux->funcao == 3){
				delay();
				aux->listaKey = getAllKeys(aux->shardID, aux->dim, aux->shardID);
			}
			sem_post(&aux->pode_resposta);
			pthread_mutex_unlock(&Mutex_s);
			p=0;
			i++;
			}
	}
	return 0;
	}
