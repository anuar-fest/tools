# shed — priority load shedding at the ingress

## Problem
At 3× normal load every request is admitted, every request queues on the
DB pool, every request times out, and the pod serves 0 % instead of 100 %
of what it can. Shedding refuses the excess early and cheaply, lowest
priority first, so the capacity that exists goes to the calls that matter.

## API
```go
s, _ := shed.New(shed.Config{Name: "api", MaxInFlight: 400})
// Critical admitted to 400, Normal to 360 (90 %), Background to 280 (70 %)
release, ok := s.Admit(ctx, shed.Normal)
if !ok { httpx.Problem(w, 503, ...) ; return }
defer release()
// HTTP: router.Use(httpx.Shed(s, classify, time.Second)) — first in the chain
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| MaxInFlight | required | From a load test: where p99 starts climbing. |
| NormalAbove | 0.9 | Normal traffic is shed when 90 % of capacity is busy. |
| BackgroundAbove | 0.7 | Background (reports, exports) yields first. |

Admission is one CAS loop; there is no TOCTOU between checking and taking a slot.

## Refuse list
- Shedding is not rate limiting. It reacts to *our* saturation, not to a client's quota.
- Do not put it after authentication: refusing must cost less than the work refused.

## Proof
`shed_test.go`: class thresholds, idempotent release, peak never exceeds the cap under 64 goroutines.

## References
Google SRE Book ch. 22 *Handling Overload*; Netflix *Performance under load*.
