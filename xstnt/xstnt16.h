#define MP_SOURCE 1
#include <string.h>
#include "xsmy.h"
#include "msgpuck.h"

/* types */
enum tp_type {
	TP_NIL = MP_NIL,
	TP_UINT = MP_UINT,
	TP_INT = MP_INT,
	TP_STR = MP_STR,
	TP_BIN = MP_BIN,
	TP_ARRAY = MP_ARRAY,
	TP_MAP = MP_MAP,
	TP_BOOL = MP_BOOL,
	TP_FLOAT = MP_FLOAT,
	TP_DOUBLE = MP_DOUBLE,
	TP_EXT = MP_EXT
};

/* header */
enum tp_header_key_t {
	TP_CODE = 0x00,
	TP_SYNC = 0x01
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
	TP_USERNAME = 0x23
};

/* response body */
enum tp_response_key_t {
	TP_DATA = 0x30,
	TP_ERROR = 0x31
};

/* request types */
enum tp_request_type {
	TP_SELECT = 1,
	TP_INSERT = 2,
	TP_REPLACE = 3,
	TP_UPDATE = 4,
	TP_DELETE = 5,
	TP_CALL = 6,
	TP_AUTH = 7,
	TP_PING = 64
};

static const uint32_t SCRAMBLE_SIZE = 20;

/* Tnt legacy structures */

typedef struct {
	size_t  size;
	char   *f;
	int     nofree;
	char    def;
} unpack_format;


typedef struct {
	U32   id;
	SV   *name;
	AV   *fields;
	unpack_format f;
} TntIndex;

typedef struct {
	U32   id;
	SV   *name;

	AV   *fields;
	HV   *indexes;
	HV   *field;

	unpack_format f;
} TntSpace;

typedef struct {
	ev_timer t;
	uint32_t id;
	void *self;
	SV * cb;
	SV * wbuf;
	U32  use_hash;
	TntSpace *space;
	unpack_format *fmt;
	unpack_format f;
	char *call;
} TntCtx;

typedef struct {
	U32  id;
	char format;
} TntField;

static inline void write_length(char *p, uint32_t size) {
	*(p) = 0xce;
	*((uint32_t *)(p+1)) = htole32(size);
}

static inline SV * pkt_ping( uint32_t iid ) {
	int sz = 5 +
		mp_sizeof_map(2) +
		mp_sizeof_uint(TP_CODE) +
		mp_sizeof_uint(TP_PING) +
		mp_sizeof_uint(TP_SYNC) +
		5;

	SV * rv = newSV(sz);
	SvUPGRADE(rv, SVt_PV);
	SvPOK_on(rv);

	char *h = (char *) SvPVX(rv);

	write_length(h, sz-5);
	h = mp_encode_map(h + 5, 2);
	h = mp_encode_uint(h, TP_CODE);
	h = mp_encode_uint(h, TP_PING);
	h = mp_encode_uint(h, TP_SYNC);
	// h = mp_encode_uint(h, iid);
	*h = 0xce;
	*(uint32_t*)(h + 1) = htole32(iid);
	h += 5;

	SvCUR_set(rv, sz);
	return rv;
}

static int parse_reply(HV *ret, const char const *data, STRLEN size, const unpack_format const * format, AV *fields) {
	return 0;
}
