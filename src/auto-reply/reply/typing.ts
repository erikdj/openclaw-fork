import { createTypingKeepaliveLoop } from "../../channels/typing-lifecycle.js";
import { createTypingStartGuard } from "../../channels/typing-start-guard.js";
import { isSilentReplyPrefixText, isSilentReplyText, SILENT_REPLY_TOKEN } from "../tokens.js";

export type TypingController = {
  onReplyStart: () => Promise<void>;
  startTypingLoop: () => Promise<void>;
  startTypingOnText: (text?: string) => Promise<void>;
  refreshTypingTtl: () => void;
  isActive: () => boolean;
  markRunComplete: () => void;
  markRunFailure: (reason?: string) => void;
  markDispatchIdle: () => void;
  setSubagentActive: (active: boolean) => void;
  refreshSubagentTtl: () => void;
  cleanup: () => void;
};

export function createTypingController(params: {
  onReplyStart?: () => Promise<void> | void;
  /** Invoked on cleanup regardless of outcome (existing callers keep this behavior). */
  onCleanup?: () => void;
  /** Invoked when the parent run completes successfully. Allows channel to swap alive-indicator to success-indicator. */
  onRunSuccess?: () => void;
  /** Invoked when the parent run ends in error. Allows channel to swap alive-indicator to failure-indicator. */
  onRunFailure?: (reason?: string) => void;
  /** Invoked when a subagent becomes active. Channel may add a subagent-indicator reaction. */
  onSubagentStart?: () => void;
  /** Invoked when the subagent stops or its TTL expires without refresh. */
  onSubagentEnd?: () => void;
  typingIntervalSeconds?: number;
  typingTtlMs?: number;
  subagentTtlMs?: number;
  silentToken?: string;
  log?: (message: string) => void;
}): TypingController {
  const {
    onReplyStart,
    onCleanup,
    onRunSuccess,
    onRunFailure,
    onSubagentStart,
    onSubagentEnd,
    typingIntervalSeconds = 6,
    typingTtlMs = 2 * 60_000,
    subagentTtlMs = 3 * 60_000,
    silentToken = SILENT_REPLY_TOKEN,
    log,
  } = params;
  if (!onReplyStart && !onCleanup) {
    return {
      onReplyStart: async () => {},
      startTypingLoop: async () => {},
      startTypingOnText: async () => {},
      refreshTypingTtl: () => {},
      isActive: () => false,
      markRunComplete: () => {},
      markRunFailure: () => {},
      markDispatchIdle: () => {},
      setSubagentActive: () => {},
      refreshSubagentTtl: () => {},
      cleanup: () => {},
    };
  }
  let started = false;
  let active = false;
  let runComplete = false;
  let dispatchIdle = false;
  type RunOutcome = "idle" | "success" | "failure";
  let runOutcome: RunOutcome = "idle";
  let runFailureReason: string | undefined;
  // Important: callbacks (tool/block streaming) can fire late (after the run completed),
  // especially when upstream event emitters don't await async listeners.
  // Once we stop typing, we "seal" the controller so late events can't restart typing forever.
  let sealed = false;
  let typingTtlTimer: NodeJS.Timeout | undefined;
  let subagentTtlTimer: NodeJS.Timeout | undefined;
  let subagentActive = false;
  const typingIntervalMs = typingIntervalSeconds * 1000;

  const formatTypingTtl = (ms: number) => {
    if (ms % 60_000 === 0) {
      return `${ms / 60_000}m`;
    }
    return `${Math.round(ms / 1000)}s`;
  };

  const resetCycle = () => {
    started = false;
    active = false;
    runComplete = false;
    dispatchIdle = false;
  };

  const clearTypingTimers = () => {
    if (typingTtlTimer) {
      clearTimeout(typingTtlTimer);
      typingTtlTimer = undefined;
    }
    if (dispatchIdleTimer) {
      clearTimeout(dispatchIdleTimer);
      dispatchIdleTimer = undefined;
    }
  };

  const clearSubagentTimer = () => {
    if (subagentTtlTimer) {
      clearTimeout(subagentTtlTimer);
      subagentTtlTimer = undefined;
    }
  };

  const cleanup = () => {
    if (sealed) {
      return;
    }
    clearTypingTimers();
    clearSubagentTimer();
    typingLoop.stop();
    if (subagentActive) {
      subagentActive = false;
      onSubagentEnd?.();
    }
    if (active) {
      // Route to outcome-specific callbacks first (channels that opted in to
      // the new success/failure handling can swap reactions here), then fall
      // back to the generic onCleanup (existing channels keep their current
      // stop-reaction behavior).
      if (runOutcome === "success") {
        onRunSuccess?.();
      } else if (runOutcome === "failure") {
        onRunFailure?.(runFailureReason);
      }
      onCleanup?.();
    }
    resetCycle();
    sealed = true;
  };

  const refreshTypingTtl = () => {
    if (sealed) {
      return;
    }
    if (!typingIntervalMs || typingIntervalMs <= 0) {
      return;
    }
    if (typingTtlMs <= 0) {
      return;
    }
    if (typingTtlTimer) {
      clearTimeout(typingTtlTimer);
    }
    typingTtlTimer = setTimeout(() => {
      if (!typingLoop.isRunning()) {
        return;
      }
      log?.(`typing TTL reached (${formatTypingTtl(typingTtlMs)}); stopping typing loop`);
      // TTL expiry stops the per-token typing loop but does NOT tear down the
      // visual alive indicator. A parent that has detached while a subagent is
      // running is genuinely idle from the typing-stream perspective, but the
      // session is still alive — we want the channel to keep its `typingReaction`
      // present until `cleanup()` is called with a real run outcome.
      typingLoop.stop();
    }, typingTtlMs);
  };

  const refreshSubagentTtl = () => {
    if (sealed) {
      return;
    }
    if (subagentTtlMs <= 0) {
      return;
    }
    if (!subagentActive) {
      return;
    }
    if (subagentTtlTimer) {
      clearTimeout(subagentTtlTimer);
    }
    subagentTtlTimer = setTimeout(() => {
      log?.(
        `subagent TTL reached (${formatTypingTtl(subagentTtlMs)}); clearing subagent indicator`,
      );
      // Subagent TTL expired without any activity events from the child. The
      // handshake reaction is removed to signal a genuine stuck state.
      if (subagentActive) {
        subagentActive = false;
        onSubagentEnd?.();
      }
    }, subagentTtlMs);
  };

  const setSubagentActive = (active: boolean) => {
    if (sealed) {
      return;
    }
    if (active && !subagentActive) {
      subagentActive = true;
      onSubagentStart?.();
      refreshSubagentTtl();
      return;
    }
    if (!active && subagentActive) {
      clearSubagentTimer();
      subagentActive = false;
      onSubagentEnd?.();
    }
  };

  const isActive = () => active && !sealed;

  const startGuard = createTypingStartGuard({
    isSealed: () => sealed,
    shouldBlock: () => runComplete,
    rethrowOnError: true,
  });

  const triggerTyping = async () => {
    await startGuard.run(async () => {
      await onReplyStart?.();
    });
  };

  const typingLoop = createTypingKeepaliveLoop({
    intervalMs: typingIntervalMs,
    onTick: triggerTyping,
  });

  const ensureStart = async () => {
    if (sealed) {
      return;
    }
    // Late callbacks after a run completed should never restart typing.
    if (runComplete) {
      return;
    }
    if (!active) {
      active = true;
    }
    if (started) {
      return;
    }
    started = true;
    await triggerTyping();
  };

  const maybeStopOnIdle = () => {
    if (!active) {
      return;
    }
    // Stop only when the model run is done and the dispatcher queue is empty.
    if (runComplete && dispatchIdle) {
      cleanup();
    }
  };

  const startTypingLoop = async () => {
    if (sealed) {
      return;
    }
    if (runComplete) {
      return;
    }
    // Always refresh TTL when called, even if loop already running.
    // This keeps typing alive during long tool executions.
    refreshTypingTtl();
    if (!onReplyStart) {
      return;
    }
    if (typingLoop.isRunning()) {
      return;
    }
    await ensureStart();
    typingLoop.start();
  };

  const startTypingOnText = async (text?: string) => {
    if (sealed) {
      return;
    }
    const trimmed = text?.trim();
    if (!trimmed) {
      return;
    }
    if (
      silentToken &&
      (isSilentReplyText(trimmed, silentToken) || isSilentReplyPrefixText(trimmed, silentToken))
    ) {
      return;
    }
    refreshTypingTtl();
    await startTypingLoop();
  };

  let dispatchIdleTimer: NodeJS.Timeout | undefined;
  const DISPATCH_IDLE_GRACE_MS = 10_000;

  const markRunComplete = () => {
    runComplete = true;
    runOutcome = "success";
    maybeStopOnIdle();
    if (!sealed && !dispatchIdle) {
      dispatchIdleTimer = setTimeout(() => {
        if (!sealed && !dispatchIdle) {
          log?.("typing: dispatch idle not received after run complete; forcing cleanup");
          cleanup();
        }
      }, DISPATCH_IDLE_GRACE_MS);
    }
  };

  const markRunFailure = (reason?: string) => {
    runComplete = true;
    runOutcome = "failure";
    runFailureReason = reason;
    if (reason) {
      log?.(`typing: run failure recorded (${reason})`);
    }
    maybeStopOnIdle();
    if (!sealed && !dispatchIdle) {
      dispatchIdleTimer = setTimeout(() => {
        if (!sealed && !dispatchIdle) {
          log?.("typing: dispatch idle not received after run failure; forcing cleanup");
          cleanup();
        }
      }, DISPATCH_IDLE_GRACE_MS);
    }
  };

  const markDispatchIdle = () => {
    dispatchIdle = true;
    if (dispatchIdleTimer) {
      clearTimeout(dispatchIdleTimer);
      dispatchIdleTimer = undefined;
    }
    maybeStopOnIdle();
  };

  return {
    onReplyStart: ensureStart,
    startTypingLoop,
    startTypingOnText,
    refreshTypingTtl,
    isActive,
    markRunComplete,
    markRunFailure,
    markDispatchIdle,
    setSubagentActive,
    refreshSubagentTtl,
    cleanup,
  };
}
