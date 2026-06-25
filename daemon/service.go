package daemon

import (
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/service"
)

const (
	binaryName  = "synckitd"
	labelPrefix = "com.github.yasyf.synckit"

	reconcileInterval = 900
)

// legacyLabels are the pre-cutover per-tool LaunchAgent labels synckitd boots out
// on install, so the one shared daemon supersedes the per-tool reconcile/watch
// agents reposync and cookiesync used to install.
var legacyLabels = []string{
	"com.github.yasyf.reposync.reconcile",
	"com.github.yasyf.reposync.watch",
	"com.github.yasyf.cookiesync.reconcile",
	"com.github.yasyf.cookiesync.watch",
}

// toolConfig builds the launchd ToolConfig for synckitd: a periodic reconcile
// tick, the resident serve daemon, and one helper agent per manifest that ships a
// Helper block. A helper agent runs the consumer's own binary (AgentSpec.Binary),
// not synckitd, scoped to the helper's launchd session type.
func toolConfig(manifests []manifest.Manifest) service.ToolConfig {
	agents := []service.AgentSpec{
		{
			Label:   "reconcile",
			Command: "reconcile",
			ExtraKeys: map[string]any{
				"StartInterval": reconcileInterval,
				"RunAtLoad":     true,
				"ProcessType":   "Background",
			},
		},
		{
			Label:   "serve",
			Command: "serve",
			ExtraKeys: map[string]any{
				"KeepAlive":   true,
				"RunAtLoad":   true,
				"ProcessType": "Background",
			},
		},
	}
	for _, m := range manifests {
		if m.Helper == nil {
			continue
		}
		agents = append(agents, service.AgentSpec{
			Label:   "helper." + m.Name,
			Command: m.Helper.Command,
			Binary:  m.Binary,
			ExtraKeys: map[string]any{
				"KeepAlive":              true,
				"RunAtLoad":              true,
				"ProcessType":            "Background",
				"LimitLoadToSessionType": m.Helper.SessionType,
			},
		})
	}
	return service.ToolConfig{
		BinaryName:  binaryName,
		LabelPrefix: labelPrefix,
		Agents:      agents,
		DaemonPATH:  service.DefaultDaemonPATH,
		LogName:     logName,
	}
}

// logName maps a full agent label to a HOME-relative log path.
func logName(agentLabel string) string {
	return "Library/Logs/synckit/" + agentLabel + ".log"
}
