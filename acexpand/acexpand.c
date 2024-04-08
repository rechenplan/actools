#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <memory.h>
#include <assert.h>
#include <time.h>

/*******************************************************************************
    
    HEADER PROCEDURES
    
********************************************************************************/

#define FILE_TYPE_OFFSET 0x140
#define BLOCK_SIZE_OFFSET 0x144
#define FILE_SIZE_OFFSET 0x148
#define DATA_SET_OFFSET 0x14C
#define DATA_SUBSET_OFFSET 0x150
#define FREE_HEAD_OFFSET 0x154
#define FREE_TAIL_OFFSET 0x158
#define FREE_COUNT_OFFSET 0x15C
#define BTREE_OFFSET 0x160

uint32_t header_get_filetype(char* header)  { return *((uint32_t*) (header + FILE_TYPE_OFFSET)); }
uint32_t header_get_blocksize(char* header) { return *((uint32_t*) (header + BLOCK_SIZE_OFFSET)); }
uint32_t header_get_filesize(char* header)  { return *((uint32_t*) (header + FILE_SIZE_OFFSET)); }
uint32_t header_get_dataset(char* header)   { return *((uint32_t*) (header + DATA_SET_OFFSET)); }
uint32_t header_get_datasubset(char* header){ return *((uint32_t*) (header + DATA_SUBSET_OFFSET)); }
uint32_t header_get_free_head(char* header) { return *((uint32_t*) (header + FREE_HEAD_OFFSET)); }
uint32_t header_get_free_tail(char* header) { return *((uint32_t*) (header + FREE_TAIL_OFFSET)); }
uint32_t header_get_freecount(char* header) { return *((uint32_t*) (header + FREE_COUNT_OFFSET)); }
uint32_t header_get_btree(char* header)     { return *((uint32_t*) (header + BTREE_OFFSET)); }

void header_set_filetype(char* header, uint32_t v)  { *((uint32_t*) (header + FILE_TYPE_OFFSET)) = v; }
void header_set_blocksize(char* header, uint32_t v) { *((uint32_t*) (header + BLOCK_SIZE_OFFSET)) = v; }
void header_set_filesize(char* header, uint32_t v)  { *((uint32_t*) (header + FILE_SIZE_OFFSET)) = v; }
void header_set_dataset(char* header, uint32_t v)   { *((uint32_t*) (header + DATA_SET_OFFSET)) = v; }
void header_set_datasubset(char* header, uint32_t v){ *((uint32_t*) (header + DATA_SUBSET_OFFSET)) = v; }
void header_set_free_head(char* header, uint32_t v) { *((uint32_t*) (header + FREE_HEAD_OFFSET)) = v; }
void header_set_free_tail(char* header, uint32_t v) { *((uint32_t*) (header + FREE_TAIL_OFFSET)) = v; }
void header_set_freecount(char* header, uint32_t v) { *((uint32_t*) (header + FREE_COUNT_OFFSET)) = v; }
void header_set_btree(char* header, uint32_t v)     { *((uint32_t*) (header + BTREE_OFFSET)) = v; }


/*******************************************************************************
    
    BLOCK PROCEDURES
    
********************************************************************************/

/*
    Grab next pointer from block
*/
uint32_t block_get_next(char* block)
{
    uint32_t next = *((uint32_t *) block);
    return next;
}

/*
    Set next pointer to block
*/
void block_set_next(char* block, uint32_t next)
{
    *((uint32_t *) block) = next;
}

/*
    Grab address of data portion of block
*/
char* block_get_data(char* block)
{
    return block + 4;
}

/*******************************************************************************
    
    DATABASE PROCEDURES
    
********************************************************************************/

/*
    Read block from database
*/
void db_read_block(
    FILE* db,                   /* Database file handle */
    uint32_t offset,            /* Offset of block to be read */
    int block_size,             /* Block size of database */
    char* buffer,               /* Buffer to read block into */
    unsigned int buffer_size)   /* Size of buffer */
{
    assert(block_size <= buffer_size);

    fseek(db, offset, SEEK_SET);
    fread(buffer, block_size, 1, db);
}

/*
    Read block from database
*/
void db_write_block(
    FILE* db,                   /* Database file handle */
    uint32_t offset,            /* Offset of block to be written to */
    int block_size,             /* Block size of database */
    char* buffer,               /* Buffer to write from */
    unsigned int buffer_size)   /* Size of buffer */
{
    assert(block_size <= buffer_size);

    fseek(db, offset, SEEK_SET);
    fwrite(buffer, block_size, 1, db);
}

void db_expand(
	FILE* db,
	int blocks_to_add)
{
	char header[1024];
	char* tail_block;
	uint32_t tail_addr, db_size, next_addr, free_count;
	int block_size;
	
	db_read_block(db, 0, 1024, header, sizeof(header));

	block_size = header_get_blocksize(header);
	db_size = header_get_filesize(header);
	tail_addr = header_get_free_tail(header);
	free_count = header_get_freecount(header);
	header_set_freecount(header, free_count + blocks_to_add);
	next_addr = db_size;
	
	tail_block = malloc(block_size);
	db_read_block(db, tail_addr, block_size, tail_block, block_size);
	while (blocks_to_add > 0) {
		block_set_next(tail_block, next_addr | 0x80000000);
		db_write_block(db, tail_addr, block_size, tail_block, block_size);

		tail_addr = next_addr;
		next_addr += block_size;
		db_size += block_size;
		blocks_to_add--;
	}
	block_set_next(tail_block, 0x80000000);
	db_write_block(db, tail_addr, block_size, tail_block, block_size);
	header_set_free_tail(header, tail_addr);
	header_set_filesize(header, db_size);
	free(tail_block);
	db_write_block(db, 0, 1024, header, sizeof(header));
}

/*******************************************************************************
    
    UTILS
    
********************************************************************************/

void util_print_header(char* header)
{
    printf("File Type:          %08X\n", header_get_filetype(header));
    printf("Block Size:         %d\n", header_get_blocksize(header));
    printf("File Size:          %d\n", header_get_filesize(header));
    printf("Free Block Head:    %08X\n", header_get_free_head(header));
    printf("Free Block Tail:    %08X\n", header_get_free_tail(header));
    printf("Free Block Count:   %d\n", header_get_freecount(header));
}

int util_count_free(FILE* db, char* header)
{
	uint32_t block_offset, count;
	unsigned char block_type;
	char* block;

	block = malloc(header_get_blocksize(header));

	count = 0;
	block_offset = header_get_free_head(header) & 0x7fffffff;
	block_type   = 1;
	while (block_offset && block_type) {
		db_read_block(db, block_offset, header_get_blocksize(header), block, header_get_blocksize(header));
		count++;
		block_offset = block_get_next(block) & 0x7fffffff;
		block_type   = block_get_next(block) >> 31;
	}
	free(block);
	return count;
}

/*******************************************************************************
    
    MAIN
    
********************************************************************************/

int main(int argc, char** argv) {
    FILE* db;
	char header[1024];
	int free_count;
	
	if (argc < 2 || argc > 3) {
		printf("Usage:\n");
		printf("acexpand <dat>             print database stats\n");
		printf("acexpand <dat> <num>       add free blocks\n");
		return 0;
	}

	db = fopen(argv[1], "rwb+");
	if (!db) {
		printf("Failed to open database.\n");
		return 0;
	}
	
	db_read_block(db, 0, 1024, header, sizeof(header));
	if (argc == 2) {
		util_print_header(header);
		free_count = util_count_free(db, header);
		printf("Manually verified %d free blocks.\n", free_count);
	} else {
		db_expand(db, atoi(argv[2]));
	}
	fclose(db);
    return 0;
}