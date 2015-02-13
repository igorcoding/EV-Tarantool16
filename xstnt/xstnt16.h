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

static inline uint32_t decode_pkt_len(char **h) {
	char *p = *h;
	uint32_t l = *((uint32_t *)(p+1));
	*h += 5;
	return be32toh(l);
}

static inline void write_length(char *h, uint32_t size) {
	*h = 0xce;
	*((uint32_t *)(h+1)) = htobe32(size);
}

static inline char * write_iid(char *h, uint32_t iid) {
	*h = 0xce;
	*(uint32_t*)(h + 1) = htobe32(iid);
	h += 5;
	return h;
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
	h = write_iid(h, iid);

	SvCUR_set(rv, sz);
	return rv;
}

static int parse_reply_hdr(HV *ret, const char const *data, STRLEN size) {
	const char *ptr, *beg, *end;

	const char *p = data;
	const char *test = p;

	// // len

	// if (mp_check(&test, data + size))
	// 	return -1;
	// if (mp_typeof(*p) != MP_UINT)
	// 	return -1;

	// uint32_t len = mp_decode_int(&p);

	// header
	test = p;
	if (mp_check(&test, data + size))
		return -1;
	if (mp_typeof(*p) != MP_MAP)
		return -2;

	uint32_t n = mp_decode_map(&p);
	uint32_t code, sync;
	while (n-- > 0) {
		if (mp_typeof(*p) != MP_UINT)
			return -3;

		uint32_t key = mp_decode_uint(&p);
		switch (key) {
			case TP_CODE:
				if (mp_typeof(*p) != MP_UINT)
					return -4;

				code = mp_decode_uint(&p);
				break;

			case TP_SYNC:
				if (mp_typeof(*p) != MP_UINT)
					return -5;

				sync = mp_decode_uint(&p);
				break;
		}
	}

	// cwarn("code = %d", code);
	// cwarn("sync = %d", sync);

	(void) hv_stores(ret, "code", newSViv(code));
	(void) hv_stores(ret, "sync", newSViv(sync));

	return p - data;
}

static int parse_reply_body(HV *ret, const char const *data, STRLEN size, const unpack_format const * format, AV *fields) {
	const char *ptr, *beg, *end;

	const char *p = data;
	const char *test = p;
	// body
	if (p == data + size) {
		return size;
	}

	test = p;
	if (mp_check(&test, data + size))
		return -1;
	if (mp_typeof(*p) != MP_MAP)
		return -1;
	int n = mp_decode_map(&p);
	while (n-- > 0) {
		uint32_t key = mp_decode_uint(&p);
		switch (key) {
		case TP_ERROR: {
			if (mp_typeof(*p) != MP_STR)
				return -1;
			uint32_t elen = 0;
			char *err_str = mp_decode_str(&p, &elen);

			(void) hv_stores(ret, "status", newSVpvs("error"));
			(void) hv_stores(ret, "errstr", newSVpvn(err_str, elen));
			break;
		}

		case TP_DATA: {
			if (mp_typeof(*p) != MP_ARRAY)
				return -1;

			(void) hv_stores(ret, "status", newSVpvs("ok"));
			// r->data = p;
			// mp_next(&p);
			// r->data_end = p;
			break;
		}
		}
		// r->bitmap |= (1ULL << key);
	}
	return p - data;
}
