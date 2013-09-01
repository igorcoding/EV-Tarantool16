#include <ev.h>
#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <inttypes.h>

#include <sys/uio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
//#include <endian.h>

#ifndef likely
#define likely(x) __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)
#endif

#define UNDEF  &PL_sv_undef

//#define MYDEBUG
#ifdef MYDEBUG
#define WHERESTR    " at %s line %d.\n"
#define WHEREARG    __FILE__, __LINE__

#define debug(fmt, ...)   do{ \
	fprintf(stderr, "[DEBG] %s:%d: ", __FILE__, __LINE__); \
	fprintf(stderr, fmt, ##__VA_ARGS__); \
	if (fmt[strlen(fmt) - 1] != 0x0a) { fprintf(stderr, "\n"); } \
	} while(0)
#else
#define debug(...)
#endif

#define cwarn(fmt, ...)   do{ \
	fprintf(stderr, "[WARN] %s:%d: ", __FILE__, __LINE__); \
	fprintf(stderr, fmt, ##__VA_ARGS__); \
	if (fmt[strlen(fmt) - 1] != 0x0a) { fprintf(stderr, "\n"); } \
	} while(0)

#ifndef croak
#define croak(fmt, ...)   do{ \
	fprintf(stderr, "[FATAL] %s:%d: ", __FILE__, __LINE__); \
	fprintf(stderr, fmt, ##__VA_ARGS__); \
	if (fmt[strlen(fmt) - 1] != 0x0a) { fprintf(stderr, "\n"); } \
	exit(255) ;\
	} while(0)
#endif

#define ctx_by_t(ptr) (ctx*)( (char*)ptr - (int) &( (ctx *)0 )->t )
#ifndef safemalloc
#define safemalloc malloc
#endif

typedef enum {
	INITIAL = 0,
	CONNECTING,
	CONNECTED,
	DISCONNECTING,
	DISCONNECTED,
	RECONNECTING,
	RESOLVING
} CnnState;

typedef struct {
	ev_io ww;
	ev_io rw;
	ev_timer tw;
	CnnState state;
	CnnState pstate;
	struct ev_loop * loop;
	
	int   sock; // from io?
	double reconnect;
	double connect_timeout;
	double rw_timeout;
	
	int addr;
	int port;
	
	struct sockaddr_in iaddr;
	struct iovec *iov;
	int           iovcnt;
	int           iovuse;
	
	char * rbuf;
	size_t ruse;
	size_t rlen;
	
	struct iovec *rd;
	int           rdcnt;
	void *any;
} Cnn;

#define set_state(newstate) do{ self->pstate = self->state; self->state = newstate; } while(0)

typedef struct {
	int reconnect_count;
} Info;

#define dSELFby(ptr,xx) Cnn * self = (Cnn*)( (char*)ptr - (char *) &( (Cnn *)0 ) -> xx )

// Methods

void do_connect(Cnn * self);
void do_disconnect(Cnn * self);
void do_reconnect(Cnn * self); //NIY
void do_enable_rw_timer(Cnn * self);
void do_disable_rw_timer(Cnn * self);

// Callbacks

void on_disconnect(Cnn *self, int err);
void on_connfail(Cnn *self, int err);
void on_connected(Cnn *self, struct sockaddr_in *peer);
void on_read(Cnn * self, size_t len);

#ifdef TEST
void on_read(Cnn * self, size_t len) {
	debug("read %zu: %-.*s",len, self->ruse, self->rbuf);
	do_disable_rw_timer(self);
}

void on_disconnect(Cnn *self, int err) {
	debug("disconnect: %s",strerror(err));
}
void on_connfail(Cnn *self, int err) {
	Info  * info = (Info *) self->any;
	debug("connfail: %s [%d]",strerror(err), ++info->reconnect_count);
	if (info->reconnect_count > 5) {
		//do_disconnect(self);
	}
}

void on_connected(Cnn *self, struct sockaddr_in *peer) {
	debug("connected address: %s:%d", inet_ntoa( peer->sin_addr ), ntohs( peer->sin_port ) );
	
	do_write(self, "GET test\n",0);
}
#endif
// Callbacks


static void on_reconnect_timer( struct ev_loop *loop, ev_timer *w, int revents ) {
	dSELFby(w,tw);
	//debug("on reconnect timer %p -> %p", w, self);
	ev_timer_stop( loop, w );
	do_connect(self);
}

void on_connect_reset(Cnn * self, int err) {
	if (self->ww.fd > -1) close(self->ww.fd);
	if (self->rw.active) ev_io_stop(self->loop,&self->rw);
	if (self->ww.active) ev_io_stop(self->loop,&self->ww);
	if (self->tw.active) ev_timer_stop(self->loop,&self->tw);
	debug("connection reset: %s (reconnect: %f)",strerror(err),self->reconnect);
	if (self->reconnect > 0) {
		set_state(RECONNECTING);
		if (self->tw.active) {
			ev_timer_stop(self->loop,&self->tw);
		}
		ev_timer_init(&self->tw,on_reconnect_timer,self->reconnect,0.);
		ev_timer_start(self->loop,&self->tw);
		on_disconnect(self,err);
	}
	else {
		set_state(INITIAL);
		on_disconnect(self,err);
	}

}

void on_connect_failed(Cnn * self, int err) {
	if (self->ww.fd > -1) close(self->ww.fd);
	if (self->rw.active) ev_io_stop(self->loop,&self->rw);
	if (self->ww.active) ev_io_stop(self->loop,&self->ww);
	if (self->tw.active) ev_timer_stop(self->loop,&self->tw);
	debug("connect failed: %s (reconnect: %f)",strerror(err),self->reconnect);
	if (self->reconnect > 0) {
		set_state(RECONNECTING);
		ev_timer_init(&self->tw,on_reconnect_timer,self->reconnect,0.);
		ev_timer_start(self->loop,&self->tw);
		on_connfail(self,err);
	}
	else {
		set_state(INITIAL);
		on_disconnect(self,err);
	}
}



static void on_read_io( struct ev_loop *loop, ev_io *w, int revents ) {
	dSELFby(w,rw);
	ssize_t rc = 0;
	debug("on rw io %p -> %p (fd: %d) (%d)", w, self, w->fd, revents);
	//rc = recv(w->fd,buf,1024,0);
	again:
	rc = read(w->fd,self->rbuf,self->rlen - self->ruse);
	debug("read: %zu (%s)",rc,strerror(errno));
	if (rc > 0) {
		self->ruse += rc;
		on_read(self,rc);
	}
	else if ( rc != 0 ) {
		switch(errno){
			case EINTR:
				goto again;
			case EAGAIN:
				return;
			default:
				ev_io_stop(loop,w);
				on_disconnect(self,errno);
		}
	}
	else {
		debug("EOF");
		on_read(self,0);
		ev_io_stop(loop,w);
		on_disconnect(self,0);
	}

/*
	char * buf[1024];
	struct iovec iov[1];
	memset(iov,0,sizeof(iov));
	iov[0].iov_base = buf;
	iov[0].iov_len = 1024;
	//rc = readv(w->fd,&iov[0],1);
	debug("recvbuf = %zu",self->rd[0].iov_len);
	rc = readv(w->fd,self->rd,self->rdcnt);
	
	debug("read: %zu (%s)",rc,strerror(errno));
	if (rc > 0) {
		on_read(self,rc);
		//debug("received %s",buf);
	}
	else if( rc != 0) {
		ev_io_stop( loop,w );
	}
	else {
		debug("EOF");
		ev_io_stop(loop,w);
	}
*/

}
static void on_rw_timer(  struct ev_loop *loop, ev_timer *w, int revents ) {
	dSELFby(w,tw);
	debug("on rw timer %p -> %p", w, self);
	ev_timer_stop( loop, w );
	on_connect_reset(self,ETIMEDOUT);
}

void do_enable_rw_timer(Cnn * self) {
	if (self->rw_timeout > 0) {
		debug("start timer %f",self->rw_timeout);
		ev_timer_start( self->loop,&self->tw );
	}
}

void do_disable_rw_timer(Cnn * self) {
	if (self->tw.active) {
		debug("stop timer %f",self->rw_timeout);
		ev_timer_stop( self->loop,&self->tw );
	}
}

static void on_write_io( struct ev_loop *loop, ev_io *w, int revents ) {
	dSELFby(w,ww);
	ssize_t wr;
	int iovcur;
	struct iovec *iov;
	debug("on ww io %p -> %p (fd: %d) [ use: %d ]", w, self, w->fd, self->iovuse);
	ev_timer_stop( self->loop,&self->tw );
	again:
	wr = writev(w->fd,self->iov,self->iovuse);
	if (wr > -1) {
		//debug("written: %zu",wr);
		for (iovcur = 0; iovcur < self->iovuse; iovcur++) {
			iov = &(self->iov[iovcur]);
			if (wr < iov->iov_len) {
				iov->iov_base += wr;
				iovcur--;
				break;
			} else {
				//debug("written [%u] %zu of %zu", iovcur, iov->iov_len, iov->iov_len);
				wr -= iov->iov_len;
			}
		}
		self->iovuse -= iovcur;
		//debug("last: %d",iovcur);
	}
	else if(wr != 0) {
		switch(errno){
			case EINTR:
				goto again;
			case EAGAIN:
				if (!w->active) {
					ev_timer_start( self->loop,&self->tw );
					ev_io_start(loop,w);
				}
				return;
			default:
				if (w->active)
					ev_io_stop(loop,w);
				on_disconnect(self,errno);
		}
	}
	else {
		if (w->active)
			ev_io_stop(loop,w);
		on_disconnect(self,0);
	}
}

void do_write(Cnn *self, char *buf, size_t len) {
	if (len == 0) len = strlen(buf);
	//debug("write %zu: %s",len,buf);
	self->iov[ self->iovuse ].iov_base = buf;
	self->iov[ self->iovuse ].iov_len = len;
	self->iovuse++;
	
	do_enable_rw_timer(self);
	on_write_io( self->loop, &self->ww,0 );
}


static void on_connect_timer ( struct ev_loop *loop, ev_timer *w, int revents ) {
	dSELFby(w,tw);
	//debug("on con timer %p -> %p", w, self);
	ev_timer_stop( loop, w );
	ev_io_stop( loop, &self->ww );
	on_connect_failed(self,ETIMEDOUT);
	return;
}

static void on_connect_io( struct ev_loop *loop, ev_io *w, int revents ) {
	dSELFby(w,ww);
	debug("on con io %p -> %p (fd: %d)", w, self, w->fd);
	
	struct sockaddr_in peer;
	socklen_t addrlen = sizeof(peer);
	
	again:
	if( getpeername( w->fd, ( struct sockaddr *)&peer, &addrlen) == 0 ) {
		
		ev_timer_stop( loop, &self->tw );
		ev_io_stop( loop, w );
		
		ev_timer_init( &self->tw,on_rw_timer,self->rw_timeout,0 );
		
		ev_io_init( &self->rw, on_read_io, self->ww.fd, EV_READ );
		ev_io_start( EV_DEFAULT, &self->rw );
		
		on_connected(self, &peer);
		
	} else {
		switch( errno ) {
			case EINTR:
				goto again;
			case EAGAIN:
				return;
			case ENOTCONN: {
				char x[1];
				recv( w->fd, x,1,0 ); // recv may give actual error
			}
			default:
				ev_timer_stop( loop, &self->tw );
				ev_io_stop( loop, w );
				on_connect_failed(self,errno);
				return;
		}
	}

}

void do_destroy(Cnn * self) {
	if (self->ww.fd > -1) close(self->ww.fd);
	if (self->rw.active) ev_io_stop(self->loop,&self->rw);
	if (self->ww.active) ev_io_stop(self->loop,&self->ww);
	if (self->tw.active) ev_timer_stop(self->loop,&self->tw);
}

void do_disconnect(Cnn * self) {
	debug("do disconnect %d",self->state);
	switch (self->state) {
		case INITIAL:
			return;
		case CONNECTED:
			// read/write buffers ?
			ev_timer_stop(self->loop,&self->tw);
			ev_io_stop(self->loop,&self->ww);
			ev_io_stop(self->loop,&self->rw);
			if (self->ww.fd > -1) close(self->ww.fd);
			return;
		case RECONNECTING:
			ev_timer_stop(self->loop,&self->tw);
			return;
		case CONNECTING:
			ev_timer_stop(self->loop,&self->tw);
			ev_io_stop(self->loop,&self->ww);
			if (self->ww.fd > -1) close(self->ww.fd);
			return;
		case DISCONNECTING:
		case DISCONNECTED:
			return;
		default:
			return;
	}
}

void do_connect(Cnn * self) {
	int sock;
	debug("connecting with timeout %f",self->connect_timeout);
	self->state = CONNECTING;
	if (self->connect_timeout > 0) {
		ev_timer_init( &self->tw, on_connect_timer, self->connect_timeout,0. );
		ev_timer_start( self->loop, &self->tw );
	}
	do {
		sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	} while (sock < 0 && errno == EINTR);
	if (sock < 0) {
		on_connect_failed(self, errno);
		return;
	}
	
	fcntl(sock, F_SETFL, O_NONBLOCK | O_RDWR);
	
	again:
	
	if (
		connect( sock, (struct sockaddr *) &self->iaddr, sizeof(self->iaddr)) == 0
		// ||  errno == EISCONN
	) {
		cwarn("connected ?");
		on_connect_io(EV_DEFAULT, &self->ww, 0);
		return;
	} else {
		//warn("connect: %s...",strerror(errno));
		switch (errno) {
			case EINPROGRESS:
			case EALREADY:
			case EWOULDBLOCK:
				// async connect now in progress
				//client->state = CLIENT_CONNECTING;
				
				break;
			case EINTR:
				goto again;
			default: {
				return on_connect_failed( self, errno );
			}
		}
	}
	//self->s = sock;
	ev_io_init( &self->ww, on_connect_io, sock, EV_WRITE );
	ev_io_start( EV_DEFAULT, &self->ww );
}

void do_check(Cnn * self) {
	if (!self->iov) croak("iovec for writing not initialized");
}

#ifdef TEST
int main () {
	struct ev_loop *loop = EV_DEFAULT;
	struct iovec iov[2];
	struct iovec rd[2];
	ev_timer live;
	
	Info info;
	
	char * readbuf[1024];
	rd[0].iov_base = readbuf;
	rd[0].iov_len = 1024;
	
	Cnn * self = (Cnn *) safemalloc(sizeof(Cnn));
	memset (self,0,sizeof(Cnn));
	
	self->iaddr.sin_family      = AF_INET;
	//self->iaddr.sin_addr.s_addr = 977100220;
	//self->iaddr.sin_port        = htons( 80 );
	self->any = &info;
	self->iaddr.sin_addr.s_addr = 0;
	self->iaddr.sin_port        = htons( 12345 );
	
	self->connect_timeout = 0.01;
	self->rw_timeout = 0.01;
	self->iov = iov;
	self->iovcnt = 2;
	self->reconnect = 1;
	
	self->rbuf = readbuf;
	self->rlen = 1024;
	self->ruse = 0;
	
	self->rd = rd;
	self->rdcnt = 1;
	
	self->loop = loop;
	
	do_connect(self);
	
	ev_loop(loop,0);

	return 0;
}
#endif
