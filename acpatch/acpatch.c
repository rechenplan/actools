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
    Write block from database
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

/*
    Grab a free block
*/
uint32_t db_alloc(
    FILE* db)
{
    char header[1024];
    char* block;
    uint32_t free_head, block_size;

    /* Read header */
    db_read_block(db, 0, 1024, header, sizeof(header));
    block_size = header_get_blocksize(header);
    free_head = header_get_free_head(header);

    if (free_head) {
        block = malloc(block_size);
        db_read_block(db, free_head, block_size, block, block_size);
        header_set_free_head(header, block_get_next(block) & 0x7fffffff);
        block_set_next(block, 0);
        db_write_block(db, free_head, block_size, block, block_size);       
        db_write_block(db, 0, 1024, header, sizeof(header));
        free(block);
    }
    return free_head;
}

/*
    Read file from database
*/
void db_read_object(
    FILE* db,                   /* Database file handle */
    uint32_t offset,            /* Offset of file to be read */
    int block_size,             /* Block size of database */
    int file_size,              /* Size of file to be read */
    char* buffer,               /* Buffer to read file into */
    unsigned int buffer_size)   /* Size of buffer */
{
    int bytes_remaining;
    char* block;

    assert(file_size <= buffer_size);
    bytes_remaining = file_size;
    block = malloc(block_size);
    
    while (bytes_remaining > 0 && offset) {
        db_read_block(db, offset, block_size, block, block_size);
        if (bytes_remaining > block_size - 4) {
            memcpy(buffer, block_get_data(block), block_size - 4);
            buffer += block_size - 4;
            bytes_remaining -= block_size - 4;
        } else {
            memcpy(buffer, block_get_data(block), bytes_remaining);
            buffer += bytes_remaining;
            bytes_remaining = 0;
        }
        offset = block_get_next(block);
    }
    free(block);
}

/*
    Write file to database
*/
void db_write_object(
    FILE* db,                   /* Database file handle */
    uint32_t offset,            /* Offset of file to be read */
    int block_size,             /* Block size of database */
    int file_size,              /* Size of file to be read */
    char* buffer,               /* Buffer to read file into */
    unsigned int buffer_size)   /* Size of buffer */
{
    int bytes_remaining;
    uint32_t next;
    char* block;
    
    assert(file_size <= buffer_size);
    bytes_remaining = file_size;
    block = malloc(block_size);
    
    while (bytes_remaining > 0 && offset) {
        db_read_block(db, offset, block_size, block, block_size);
        if (bytes_remaining > block_size - 4) {
            memcpy(block_get_data(block), buffer, block_size - 4);
            buffer += block_size - 4;
            bytes_remaining -= block_size - 4;
        } else {
            memcpy(block_get_data(block), buffer, bytes_remaining);
            buffer += bytes_remaining;
            bytes_remaining = 0;
        }
        next = block_get_next(block);
        if (!next && bytes_remaining) {
            next = db_alloc(db);
            block_set_next(block, next);
        }
        db_write_block(db, offset, block_size, block, block_size);
        offset = next;
    }
    
    free(block);
}

/*******************************************************************************
    
    DIRECTORY PROCEDURES
    
********************************************************************************/

#define DIRECTORY_SIZE 1716
#define MAX_BRANCH 62

uint32_t dir_is_leaf(
    char* dir)
{
    return *((uint32_t*) dir) == 0;
}

uint32_t dir_entry_count(
    char* dir)
{
    return *((uint32_t*)(dir + (MAX_BRANCH * sizeof(uint32_t))));
}

uint32_t dir_get_branch(
    char* dir,
    int branch_ix)
{
    assert(!dir_is_leaf(dir));
    assert(branch_ix < MAX_BRANCH);
    assert(branch_ix < dir_entry_count(dir) + 1);
    return *((uint32_t*)(dir + (branch_ix * sizeof(uint32_t))));
}

#define ENTRY_BITFLAGS      0
#define ENTRY_OBJECTID      1
#define ENTRY_FILEOFFSET    2
#define ENTRY_FILESIZE      3
#define ENTRY_DATE          4
#define ENTRY_VERSION       5

uint32_t* dir_get_entry(
    char* dir,
    int entry_ix)
{
        assert(entry_ix < dir_entry_count(dir));
        return (uint32_t*)(dir + ((MAX_BRANCH + 1) * sizeof(uint32_t)) + entry_ix * 24);
}

/*******************************************************************************
    
    CRAWLER PROCEDURES
    
********************************************************************************/

typedef uint32_t callback_t(uint32_t* entry, void* params);

/*
    Returns 1 if halted early
*/
int crawl_r(
    FILE* db,
    uint32_t block_size,
    uint32_t dir_addr,
    callback_t cb,
    void* params)
{
    int branch_ix, entry_ix;
    char dir[DIRECTORY_SIZE];
    uint32_t* entry;
    int entry_count;
    
    /* Read current directory */
    db_read_object(db, dir_addr, block_size, DIRECTORY_SIZE, dir, sizeof(dir));
    entry_count = dir_entry_count(dir);

    /* If this directory isn't a leaf node, recurse all other subdirectories */
    if (!dir_is_leaf(dir)) {
        for (branch_ix = 0; branch_ix < entry_count + 1; branch_ix++) {
            uint32_t branch_addr = dir_get_branch(dir, branch_ix);
            if (crawl_r(db, block_size, branch_addr, cb, params))
                return 1;
        }
    }

    /* Iterate over contents of this directory */
    for (entry_ix = 0; entry_ix < entry_count; entry_ix++) {
        entry = dir_get_entry(dir, entry_ix);
        if (cb(entry, params)) {
            db_write_object(db, dir_addr, block_size, DIRECTORY_SIZE, dir, sizeof(dir));
            return 1;
        }
    }
    return 0;
}

/*
    Returns 1 if halted early
*/
int crawl(
    FILE* db,
    char* header,
    callback_t cb,
    void* params)
{
    /* Call recursive subroutine */
    return crawl_r(db, header_get_blocksize(header), header_get_btree(header), cb, params);
}

uint32_t cb_print(uint32_t* entry, void* params) {
    printf("%08X %08X %08X %08X %d\n",
        entry[ENTRY_OBJECTID],
        entry[ENTRY_BITFLAGS],
        entry[ENTRY_VERSION],
        entry[ENTRY_FILEOFFSET],
        entry[ENTRY_FILESIZE]);
        
    return 0;
}

uint32_t cb_find(uint32_t* entry, void* params) {
    uint32_t* newEntry = (uint32_t*) params;
    uint32_t r = 0;
    if (entry[ENTRY_OBJECTID] == newEntry[ENTRY_OBJECTID]) {
        newEntry[ENTRY_BITFLAGS]    = entry[ENTRY_BITFLAGS];
        newEntry[ENTRY_OBJECTID]    = entry[ENTRY_OBJECTID];
        newEntry[ENTRY_FILEOFFSET]  = entry[ENTRY_FILEOFFSET];
        newEntry[ENTRY_FILESIZE]    = entry[ENTRY_FILESIZE];
        newEntry[ENTRY_DATE]        = entry[ENTRY_DATE];
        newEntry[ENTRY_VERSION]     = entry[ENTRY_VERSION];
        r = 1;
    }
    return r;
}

uint32_t cb_replace(uint32_t* entry, void* params) {
    uint32_t* newEntry = (uint32_t*) params;
    if (entry[ENTRY_OBJECTID] == newEntry[ENTRY_OBJECTID]) {
        entry[ENTRY_BITFLAGS]    = newEntry[ENTRY_BITFLAGS];
        entry[ENTRY_OBJECTID]    = newEntry[ENTRY_OBJECTID];
        entry[ENTRY_FILEOFFSET]  = newEntry[ENTRY_FILEOFFSET];
        entry[ENTRY_FILESIZE]    = newEntry[ENTRY_FILESIZE];
        entry[ENTRY_DATE]        = newEntry[ENTRY_DATE];
        entry[ENTRY_VERSION]     = newEntry[ENTRY_VERSION];
        return 1;
   }
   return 0;
}

/*******************************************************************************
    
    UTILS
    
********************************************************************************/

void util_print_header(char* header)
{
    printf("File Type:          %08X\n", header_get_filetype(header));
    printf("Block Size:         %d\n", header_get_blocksize(header));
    printf("File Size:          %d\n", header_get_filesize(header));
    printf("Data Set:           %08X\n", header_get_dataset(header));
    printf("Data Subset:        %08X\n", header_get_datasubset(header));
    printf("Free Block Head:    %08X\n", header_get_free_head(header));
    printf("Free Block Tail:    %08X\n", header_get_free_tail(header));
    printf("Free Block Count:   %d\n", header_get_freecount(header));
}

int util_find_object(
    FILE* db,
    char* header,
    uint32_t object_id,
    uint32_t* entry /* out */)
{
    entry[ENTRY_OBJECTID] = object_id;
    return crawl(db, header, cb_find, entry);
}

int util_replace_entry(
    FILE* db,
    char* header,
    uint32_t* entry)
{
    return crawl(db, header, cb_replace, entry);
}


void util_print_objects(
    FILE* db)
{
    char header[1024];

    /* Read header */
    db_read_block(db, 0, 1024, header, sizeof(header));

    /* Print objects */
    crawl(db, header, cb_print, NULL);
}

void util_export_object(
    FILE* db,
    char* object_id_str,
	char* to_file_str)
{
    FILE* out;
    uint32_t entry[6];
    uint32_t object_id;
    char* buffer;
    char header[1024];

    /* Read header */
    db_read_block(db, 0, 1024, header, sizeof(header));

    /* Parse object id from object id string */
    sscanf(object_id_str, "%08X", &object_id);
    
    /* Find object */
    if (util_find_object(db, header, object_id, entry)) {
        
        /* Export object to file */
        buffer = malloc(entry[ENTRY_FILESIZE]);
        db_read_object(
            db,
            entry[ENTRY_FILEOFFSET],
            header_get_blocksize(header),
            entry[ENTRY_FILESIZE],
            buffer,
            entry[ENTRY_FILESIZE]);
        
        out = fopen(to_file_str, "wb");
        fwrite(buffer, entry[ENTRY_FILESIZE], 1, out);
        fclose(out);
        free(buffer);
        
    } else {
        printf("Object not found.\n");
    }
}

void util_replace_object(
    FILE* db,
    char* object_id_str,
	char* from_file_str)
{
    FILE* out;
    uint32_t size;
    uint32_t entry[6];
    uint32_t object_id;
    uint32_t free_count;
    uint32_t block_size;
    char* buffer;
    char header[1024];

    /* Read header */
    db_read_block(db, 0, 1024, header, sizeof(header));
    block_size = header_get_blocksize(header);
    free_count = header_get_freecount(header);

    /* Parse object id from object id string */
    sscanf(object_id_str, "%08X", &object_id);
    
    /* Find object */
    if (util_find_object(db, header, object_id, entry)) {
        
        /* Read entire file into buffer */
        out = fopen(from_file_str, "rb");
        if (out) {
            fseek(out, 0, SEEK_END);
            size = ftell(out);
            fseek(out, 0, SEEK_SET);        
            buffer = malloc(size);
            fread(buffer, size, 1, out);
            fclose(out);
        } else {
            printf("Unable to load replacement object.\n");
            return;
        }
        
        if ((int)(size - entry[ENTRY_FILESIZE]) > (int)((block_size - 4) * free_count)) {
            printf("Not enough space in database. Try expanding.\n");
            free(buffer);
            return;
        }

        db_write_object(
            db,
            entry[ENTRY_FILEOFFSET],
            header_get_blocksize(header),
            size,
            buffer,
            size);
            
        free(buffer);
        
        if (entry[ENTRY_FILESIZE] != size) {
            entry[ENTRY_FILESIZE] = size;
            util_replace_entry(db, header, entry);
        }
        
    } else {
        printf("Original object not found in database.\n");
    }
}       


/*******************************************************************************
    
    MAIN
    
********************************************************************************/

int main(int argc, char** argv) {
    FILE* db;
    if (argc < 3 || argc > 5) {
        printf("Usage:\n");
        printf("acpatch l <datfile>                       list contents of database\n");
        printf("acpatch x <datfile> <object> <tofile>     export object\n");
        printf("acpatch r <datfile> <object> <fromfile>   replace object\n");
        return 0;
    }

    db = fopen(argv[2], "rwb+");
    if (!db) {
        printf("Failed to open database.\n");
        return 0;
    }
    
    switch (argv[1][0]) {
        case 'l':
            util_print_objects(db);
            break;
        case 'x':
            util_export_object(db, argv[3], argv[4]);
            break;
        case 'r':
            util_replace_object(db, argv[3], argv[4]);
            break;
        default:
            printf("Invalid mode.\n");
            break;
    }
    return 0;
}