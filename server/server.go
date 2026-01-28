package server

import (
	"context"
	"io"
	"log"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ConnectionEvent interface {
	proto.Message
}

type EventHandler[T ConnectionEvent] interface {
	Handle(ctx context.Context, event T) error
	OnStreamStart(ctx context.Context) error
	OnStreamEnd(ctx context.Context) error
}

type StreamReceiver[T ConnectionEvent] interface {
	Recv() (T, error)
	Context() context.Context
}

type BeamServer[T ConnectionEvent] struct {
	handler     EventHandler[T]
	concurrency int
	mu          sync.RWMutex
	stats       StreamStats
}

type StreamStats struct {
	TotalStreams   int64
	ActiveStreams  int64
	EventsReceived int64
	EventsErrors   int64
}

func NewBeamServer[T ConnectionEvent](handler EventHandler[T], concurrency int) *BeamServer[T] {
	if concurrency <= 0 {
		concurrency = 1
	}

	return &BeamServer[T]{
		handler:     handler,
		concurrency: concurrency,
	}
}

func (s *BeamServer[T]) HandleStream(stream StreamReceiver[T]) error {
	ctx := stream.Context()

	s.mu.Lock()
	s.stats.TotalStreams++
	s.stats.ActiveStreams++
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.stats.ActiveStreams--
		s.mu.Unlock()
	}()

	if err := s.handler.OnStreamStart(ctx); err != nil {
		return status.Errorf(codes.Internal, "stream start failed: %v", err)
	}

	defer func() {
		if err := s.handler.OnStreamEnd(ctx); err != nil {
			log.Printf("stream end handler error: %v", err)
		}
	}()

	eventCh := make(chan T, s.concurrency)
	errCh := make(chan error, 1)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	var wg sync.WaitGroup
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.processEvents(workerCtx, eventCh, errCh)
		}()
	}

	recvErr := make(chan error, 1)

	go func() {
		defer close(eventCh)

		for {
			event, err := stream.Recv()
			if err == io.EOF {
				log.Println("Client closed stream gracefully (EOF)")
				recvErr <- nil
				return
			}
			if err != nil {
				if status.Code(err) == codes.Canceled {
					log.Println("Stream cancelled (expected on client disconnect)")
					recvErr <- nil
					return
				}
				log.Printf("Stream receive error: %v", err)
				recvErr <- err
				return
			}

			s.mu.Lock()
			s.stats.EventsReceived++
			s.mu.Unlock()

			select {
			case eventCh <- event:
			case <-workerCtx.Done():
				log.Println("Context cancelled during receive")
				recvErr <- nil
				return
			}
		}
	}()

	err := <-recvErr

	workerCancel()
	wg.Wait()

	if err != nil {
		if err == context.Canceled {
			return status.Errorf(codes.Canceled, "stream cancelled: %v", err)
		}
		return status.Errorf(codes.Internal, "stream error: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			return status.Errorf(codes.Internal, "processing error: %v", err)
		}
	default:
	}

	return nil
}

func (s *BeamServer[T]) processEvents(ctx context.Context, eventCh <-chan T, errCh chan<- error) {
	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				return
			}

			if err := s.handler.Handle(ctx, event); err != nil {
				s.mu.Lock()
				s.stats.EventsErrors++
				s.mu.Unlock()

				log.Printf("error handling event: %v", err)

				select {
				case errCh <- err:
				default:
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *BeamServer[T]) GetStats() StreamStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}
