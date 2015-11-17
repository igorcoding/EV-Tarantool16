#ifndef _TYPES_H_
#define _TYPES_H_

/* header */
enum tp_header_key_t {
	TP_CODE = 0x00,
	TP_SYNC = 0x01,
	TP_SERVER_ID = 0x02,
	TP_LSN = 0x03,
	TP_TIMESTAMP = 0x04,
	TP_SCHEMA_ID = 0x05,
};

/* request body */
enum tp_body_key_t {
	TP_SPACE = 0x10,
	TP_INDEX = 0x11,
	TP_LIMIT = 0x12,
	TP_OFFSET = 0x13,
	TP_ITERATOR = 0x14,
	TP_KEY = 0x20,
	TP_TUPLE = 0x21,
	TP_FUNCTION = 0x22,
	TP_USERNAME = 0x23,
	TP_EXPRESSION = 0x27
};

/* response body */
enum tp_response_key_t {
	TP_DATA = 0x30,
	TP_ERROR = 0x31
};

/* request types */
enum tp_request_type {
	TP_SELECT = 0x01,
	TP_INSERT = 0x02,
	TP_REPLACE = 0x03,
	TP_UPDATE = 0x04,
	TP_DELETE = 0x05,
	TP_CALL = 0x06,
	TP_AUTH = 0x07,
	TP_EVAL = 0x08,
	TP_PING = 0x40
};

typedef struct {
	int code;
	int id;
	int schema_id;
} tnt_header_t;

void tnt_header_init(tnt_header_t *hdr) {
	hdr->code = -1;
	hdr->id = -1;
	hdr->schema_id = -1;
}

typedef struct {
	size_t  size;
	char   *f;
	int     nofree;
	char    def;
} unpack_format;


typedef struct {
	U32   id;
	SV   *name;
	SV   *type;
	HV   *opts;
	AV   *fields;
	unpack_format f;
} TntIndex;

typedef struct {
	U32   id;
	SV   *name;
	SV   *owner;
	SV   *engine;
	SV   *fields_count;
	SV   *flags;

	AV   *fields;
	HV   *indexes;
	HV   *field;

	unpack_format f;
} TntSpace;

typedef struct {
	ev_timer t;
	uint32_t id;
	void *self;
	SV *cb;
	SV *wbuf;
	U32 use_hash;
	uint8_t log_level;
	TntSpace *space;
	unpack_format *fmt;
	unpack_format f;
	char *call;
} TntCtx;

typedef struct {
	U32  id;
	char format;
	SV   *name;
} TntField;

typedef enum {
	OP_UPD_ARITHMETIC,
	OP_UPD_DELETE,
	OP_UPD_INSERT_ASSIGN,
	OP_UPD_SPLICE,
	OP_UPD_UNKNOWN
} update_op_type_t;

typedef enum {
	FMT_UNKNOWN = '*',
	FMT_NUM = 'n',
	FMT_STR = 's',
	FMT_NUMBER = 'b',
	FMT_INT = 'i',
	FMT_ARRAY = 'a'
} TNT_FORMAT_TYPE;

typedef enum {
	TNT_IT_EQ = 0,
	TNT_IT_REQ = 1,
	TNT_IT_ALL = 2,
	TNT_IT_LT = 3,
	TNT_IT_LE = 4,
	TNT_IT_GE = 5,
	TNT_IT_GT = 6,
	TNT_IT_BITS_ALL_SET = 7,
	TNT_IT_BITS_ANY_SET = 8,
	TNT_IT_BITS_ALL_NOT_SET = 9,
	TNT_IT_OVERLAPS = 10,
	TNT_IT_NEIGHBOR = 11,
} tnt_iterator_t;


#endif // _TYPES_H_
