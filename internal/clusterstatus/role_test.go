package clusterstatus

import "github.com/p1u3o/kvlock/internal/elect"

func httpapiRole(s string) elect.Role { return elect.Role(s) }
