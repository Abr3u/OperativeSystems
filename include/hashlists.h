#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <semaphore.h>
#include "kos_client.h"
#define HT_SIZE 10
#define KV_SIZE 20

typedef struct kv_item{
	KV_t* pair;
	struct kv_item* next;
}kv_item;

typedef struct{
	kv_item* hash_linha[HT_SIZE];
	int compress;
}hashtable;

typedef enum { FALSE, TRUE } boolean_t;

typedef struct{
	sem_t leitores;
	sem_t escritores;
	pthread_mutex_t mutex;
	boolean_t escrevendo;
	int leitores_espera;
	int escritores_espera;
	int nleitores;
}shardbuffer;

hashtable* hash_table_new();
kv_item* create_kv(char* key, char* value);
char* hash_table_remove(hashtable *hash_kv, char *key, int shardid);
char* hash_table_get(hashtable *hash_kv, char *key, int shardid);
int hash(char* key);
kv_item* check_existance(hashtable *hash_kv, char *key);
char* kv_hash_insert(hashtable* hash_kv, char* key, char* value, int shardid);
int conta_shard(int shardid);
KV_t* getAllKeys(int shardId, int *dim, int shardid);
void reinicia(hashtable *hash_kv, int shardid);
void insert_infile(hashtable* hash_kv, int shardid, char *key, char *value, char* overwrite);
void remove_offile(hashtable* hash_kv, int shardid, char *key);
void read_init(int shardid);
void read_end(int shardid);
void write_init(int shardid);
void write_end(int shardid);
void hash_file_compress(hashtable* hash_kv,int shardid);
