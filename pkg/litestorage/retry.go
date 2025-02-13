package litestorage

// func (s *LiteStorage) withRetry(operation func() error) error {
// 	b := backoff.NewExponentialBackOff()
// 	b.MaxElapsedTime = 30 * time.Second

// 	return backoff.Retry(func() error {
// 		err := operation()
// 		if err != nil {
// 			if errors.Is(err, badger.ErrConflict) {
// 				return err // Retryable error
// 			}
// 			return backoff.Permanent(err) // Non-retryable error
// 		}
// 		return nil
// 	}, b)
// }