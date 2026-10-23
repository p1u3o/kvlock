package controller

// AddEventForTest adds an event (tests only).
func AddEventForTest(c *Controller, kind, msg string) { c.event(kind, msg) }
