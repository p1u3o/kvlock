package controller

import (
	"context"

	"github.com/p1u3o/kvlock/internal/backup"
	"github.com/p1u3o/kvlock/internal/elect"
)

// BackupRunner is the part of backup.Runner the controller drives.
type BackupRunner interface {
	Run(ctx context.Context, state func() backup.NodeState)
	Status() backup.Status
	SetEventFunc(func(kind, msg string))
}

// startBackups runs the backup loop in active mode only; observe mode
// must never BGSAVE or upload.
func (c *Controller) startBackups(ctx context.Context) {
	if c.deps.Backup == nil || c.cfg.Mode == "observe" {
		return
	}
	c.deps.Backup.SetEventFunc(c.event)
	go c.deps.Backup.Run(ctx, c.backupState)
}

// backupState is the node state the backup runner decides on.
func (c *Controller) backupState() backup.NodeState {
	s := c.Snapshot()
	c.mu.RLock()
	defer c.mu.RUnlock()
	st := backup.NodeState{
		Leader:      c.role == elect.RoleLeader,
		Follower:    c.role == elect.RoleFollower,
		Healthy:     s.Healthy && c.kvUp && c.sessionValid,
		BackendRole: c.backendRole,
		Session:     c.sessionID,
	}
	if ri := c.lastRI; ri != nil {
		st.MasterLinkUp = ri.Role == "slave" && ri.MasterLinkUp
		st.Replicas = len(ri.Replicas)
	}
	if c.hb.havePeer {
		st.LagOK = c.hb.lag >= 0 && c.hb.lag <= c.cfg.MaxLagTime
	} else {
		st.LagOK = c.lag >= 0 && c.lag <= c.cfg.MaxLagBytes
	}
	if !st.Healthy {
		st.Session = ""
	}
	return st
}
