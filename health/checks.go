package health

import (
	"context"
	"errors"
)

// Bool adapts a boolean probe such as natsjs.Client.Healthy; failure is the
// error text when it returns false.
func Bool(ok func() bool, failure string) Check {
	err := errors.New(failure)
	return func(context.Context) error {
		if ok() {
			return nil
		}
		return err
	}
}
