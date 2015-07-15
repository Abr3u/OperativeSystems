	#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include "kos_client.h"
#include "hashlists.h"
#include "server.h"

kv_item* create_kv(char *key, char *value){
	kv_item *new_item;
	KV_t *pair;
	pair = (KV_t*)malloc(1*sizeof(KV_t));
	new_item = (kv_item*)malloc(1*sizeof(kv_item));
	strcpy(pair->key,key);
	strcpy(pair->value,value);
	new_item->pair = pair;
	new_item->next = NULL;
	return new_item;
}

hashtable* hash_table_new(){
	hashtable* hash_kv = (hashtable*)malloc(1*sizeof(hashtable));
	int i;
	for (i=0; i<HT_SIZE; i++)
		hash_kv->hash_linha[i] = NULL;
	hash_kv->compress=50;
		
	return hash_kv;
	}

kv_item* check_existance(hashtable* hash_kv, char *key){
	int i = hash(key);

	if(hash_kv->hash_linha[i] == NULL)
		return NULL;
	else{
		kv_item* aux = hash_kv->hash_linha[i];
		while(aux->next != NULL && strcmp(aux->pair->key, key) != 0)
			aux = aux->next;
		if(strcmp(aux->pair->key, key) == 0)
			return aux;
		else	
		return NULL;
	}
}

int hash(char* key){
     
    int i=0;
 
    if (key == NULL)
        return -1;
 
    while (*key != '\0') {
        i+=(int) *key;
        key++;
    }
 
    i=i % HT_SIZE;
 
    return i;
}

char* kv_hash_insert(hashtable* hash_kv, char* key, char* value, int shardid){
	write_init(shardid);
	kv_item *new;
	char *ret = (char*)malloc(KV_SIZE*sizeof(char));
	
	int i=hash(key);
	if(check_existance(hash_kv, key) != NULL){
		new = check_existance(hash_kv, key);
		strcpy(ret, new->pair->value);
		strcpy(new->pair->value, value);
		hash_kv->compress--;
		hash_file_compress(hash_kv, shardid);
		write_end(shardid);
		return ret;
	}
	else{
		new = create_kv(key, value);
		if(hash_kv->hash_linha[i] == NULL){
			hash_kv->hash_linha[i] = new;
			hash_kv->compress--;
			hash_file_compress(hash_kv, shardid);
			write_end(shardid);
			return NULL;
			}
		else{
			new->next = hash_kv->hash_linha[i];
			hash_kv->hash_linha[i] = new;
			hash_kv->compress--;
			hash_file_compress(hash_kv, shardid);
			write_end(shardid);
			return NULL;
			}
		}
	}
	
char* hash_table_remove(hashtable *hash_kv, char *key, int shardid){
	write_init(shardid);
	kv_item *aux, *aux1;
	char *ret;
	int i=hash(key);
	if(check_existance(hash_kv, key) == NULL){
		write_end(shardid);
		return NULL;
	}
	else{
		aux = hash_kv->hash_linha[i];
		if(strcmp(aux->pair->key, key) == 0){
			hash_kv->hash_linha[i] = aux->next;
			ret = aux->pair->value;
			free(aux);
			hash_kv->compress--;
			hash_file_compress(hash_kv, shardid);
			write_end(shardid);
			return ret;
		}
		else{
			while(strcmp(aux->next->pair->key, key) != 0)
				aux = aux->next;
		
			aux1 = aux->next;
			ret = aux1->pair->value;
			aux->next = aux1->next;
			free(aux1);
			hash_kv->compress--;
			hash_file_compress(hash_kv, shardid);
			write_end(shardid);
			return ret;
		}
	}
}

char* hash_table_get(hashtable *hash_kv, char *key, int shardid){
	read_init(shardid);
	kv_item *aux;
	aux = check_existance(hash_kv, key);
	if(aux == NULL){
		read_end(shardid);
		return NULL;
		}
	else{
		read_end(shardid);
		return aux->pair->value;
		}
	}
	
int conta_shard(int shardid){
	hashtable* shard = database[shardid];
	kv_item *aux;
	int i, elementos=0;
	for(i=0;i<HT_SIZE;i++){
		aux = shard->hash_linha[i];
		while(aux!=NULL){
			elementos++;
			aux = aux->next;
			}
		}
	return elementos;
	}
	
KV_t* getAllKeys(int shardId, int *dim, int shardid){
	read_init(shardid);
	hashtable* shard = database[shardId];
	KV_t *aux;
	kv_item *aux2;
	int i, elementos = conta_shard(shardId), j=0;
	KV_t* allkeys = (KV_t*)malloc(elementos*sizeof(KV_t));
	for(i=0;i<HT_SIZE;i++){
		aux2 = shard->hash_linha[i];
		while(aux2!=NULL && j<elementos){
			aux = aux2->pair;
			allkeys[j] = *aux;
			j++;
			aux2 = aux2->next;
			}
		}
	dim = &elementos;
	read_end(shardid);
	return allkeys;
	}

void reinicia(hashtable *hash_kv, int shardid){
	char nome_arquivo[100];
	char key[KV_SIZE], value[KV_SIZE];
	FILE *fd;
	sprintf(nome_arquivo, "f%d", shardid);
	fd = fopen(nome_arquivo,"a+");
	if (!fd)
		return;
	else{
		while(fscanf(fd,"%s %s", key, value) == 2)
			kv_hash_insert(hash_kv, key, value, shardid);
		fclose(fd);
		}
	}

void insert_infile(hashtable* hash_kv, int shardid, char *key, char *value, char* overwrite){
	char nome_arquivo[100];
	sprintf(nome_arquivo, "f%d", shardid);
	if(overwrite == NULL){
		FILE *fd = fopen(nome_arquivo,"a");
		if (!fd){
			printf ("Erro na abertura do arquivo. Fim de programa.");
			exit (1);
			}
		
		fprintf(fd,"%s %s\n", key, value);
		//close the file
		fclose(fd);
		}
	else{
		FILE *fd = fopen(nome_arquivo,"w+");
		if (!fd){
			printf ("Erro na abertura do arquivo. Fim de programa.");
			exit (1);
        }
        char key1[KV_SIZE], value1[KV_SIZE];
		while(fscanf(fd,"%s %s", key1, value1) == 2){
			if(strcmp(key1, key) == 0)
				fprintf(fd,"%s %s\n", key1, value);
				}
		fclose(fd);
		}
	}

void remove_offile(hashtable* hash_kv, int shardid, char *key){
	kv_item* kv = check_existance(hash_kv, key);
	char* vazio = calloc(KV_SIZE,sizeof(char));
	if(kv != NULL){
		char nome_arquivo[100];
		sprintf(nome_arquivo, "f%d", shardid);
		FILE *fd = fopen(nome_arquivo,"w+");
		if (!fd){
			printf ("Erro na abertura do arquivo. Fim de programa.");
			exit (1);
			}
		char key1[KV_SIZE], value1[KV_SIZE];
		while(fscanf(fd,"%s %s", key1, value1) == 2){
			if(strcmp(key1, key) == 0){
				fprintf(fd,"%s %s\n", key1, vazio);
				}
			}
		fclose(fd);
	}
}

void hash_file_compress(hashtable* hash_kv, int shardid){
	if (hash_kv->compress == 0){
	char nome_arquivo[100];
	char* vazio = calloc(KV_SIZE,sizeof(char));
	sprintf(nome_arquivo, "f%d", shardid);
	FILE *fd = fopen(nome_arquivo,"r"), *p = fopen("auxiliar.txt","w");
	if (!fd || !p){
		printf ("Erro na abertura do arquivo. Fim de programa.");
		exit (1);
		}
	char key1[KV_SIZE], value1[KV_SIZE];
	while(fscanf(fd,"%s %s",key1, value1) == 2){
		if(strcmp(value1, vazio) != 0){
			fprintf(p,"%s %s\n", key1, value1);
			}
		}
	fclose(fd);
	fclose(p);
	fd = fopen(nome_arquivo,"w");
	p = fopen("auxiliar.txt","r");
	while(fscanf(p,"%s %s", key1, value1) == 2)
		fprintf(fd,"%s %s\n", key1, value1);
	
	fclose(p);
	fclose(fd);
}
}

void read_init(int shardid){
	pthread_mutex_lock(&shardbuf[shardid].mutex);
	if(shardbuf[shardid].escrevendo || shardbuf[shardid].escritores_espera > 0){
		shardbuf[shardid].leitores_espera++;
		pthread_mutex_unlock(&shardbuf[shardid].mutex);
		sem_wait(&shardbuf[shardid].leitores);
		pthread_mutex_lock(&shardbuf[shardid].mutex);
		shardbuf[shardid].leitores_espera--;
		}
	else
		shardbuf[shardid].nleitores++;
	pthread_mutex_unlock(&shardbuf[shardid].mutex);
	}

void read_end(int shardid){
	pthread_mutex_lock(&shardbuf[shardid].mutex);
	shardbuf[shardid].nleitores--;
	if(shardbuf[shardid].nleitores == 0 && shardbuf[shardid].escritores_espera > 0){
		sem_post(&shardbuf[shardid].escritores);
		shardbuf[shardid].escrevendo = TRUE;
		shardbuf[shardid].escritores_espera--;	
		}
	pthread_mutex_unlock(&shardbuf[shardid].mutex);
	}

void write_init(int shardid){
	pthread_mutex_lock(&shardbuf[shardid].mutex);
	if(shardbuf[shardid].escrevendo || shardbuf[shardid].nleitores>0){
		shardbuf[shardid].escritores_espera++;
		pthread_mutex_unlock(&shardbuf[shardid].mutex);
		sem_wait(&shardbuf[shardid].escritores);
		pthread_mutex_lock(&shardbuf[shardid].mutex);
		shardbuf[shardid].escritores_espera--;
		}
	shardbuf[shardid].escrevendo=TRUE;
	pthread_mutex_unlock(&shardbuf[shardid].mutex);
	}
	
void write_end(int shardid){
	int i;
	pthread_mutex_lock(&shardbuf[shardid].mutex);
	shardbuf[shardid].escrevendo = FALSE;
	if(shardbuf[shardid].leitores_espera > 0){
		for(i=0;i<shardbuf[shardid].leitores_espera;i++){
			sem_post(&shardbuf[shardid].leitores);
			shardbuf[shardid].nleitores++;
			}
		shardbuf[shardid].leitores_espera = shardbuf[shardid].leitores_espera - i;
		}
	else if(shardbuf[shardid].escritores_espera > 0){
		sem_post(&shardbuf[shardid].escritores);
		shardbuf[shardid].escrevendo = TRUE;
		shardbuf[shardid].escritores_espera--;
		}
	pthread_mutex_unlock(&shardbuf[shardid].mutex);
	}
