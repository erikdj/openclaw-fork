import { createTypingKeepaliveLoop } from "./typing-lifecycle.js";
import { createTypingStartGuard } from "./typing-start-guard.js";

export type TypingCallbacks = {
  onReplyStart: () => Promise<void>;
  onIdle?: () => void;
  /** Called when the typing controller is cleaned up (e.g. on NO_REPLY). */
  onCleanup?: () => void;
  /** Called on successful run completion — channel may swap to a success reaction. */
  onRunSuccess?: () => void;
  /** Called on run failure — channel may swap to an error reaction. */
  onRunFailure?: (reason?: string) => void;
  /** Called when a subagent becomes active — channel may add a subagent reaction. */
  onSubagentStart?: () => void;
  /** Called when the subagent stops or its TTL expires — channel should remove the subagent reaction. */
  onSubagentEnd?: () => void;
};

export type CreateTypingCallbacksParams = {
  start: () => Promise<void>;
  stop?: () => Promise<void>;
  onStartError: (err: unknown) => void;
  onStopError?: (err: unknown) => void;
  keepaliveIntervalMs?: number;
  /** Stop keepalive after this many consecutive start() failures. Default: 2 */
  maxConsecutiveFailures?: number;
  /** Maximum duration for typing indicator before auto-cleanup (safety TTL). Default: 60s */
  maxDurationMs?: number;
  /**
   * Invoked on successful run completion. Channel-specific handler — typically
   * removes the typingReaction (if any) and adds a completionReaction.
   */
  onRunSuccess?: () => Promise<void> | void;
  /**
   * Invoked when a run ends with an error. Channel-specific handler — typically
   * removes the typingReaction (if any) and adds an errorReaction.
   */
  onRunFailure?: (reason?: string) => Promise<void> | void;
  /**
   * Invoked when a subagent becomes active under the parent session. Channel-specific
   * handler — typically adds a subagentReaction (the typingReaction may remain in
   * place or be swapped out, per channel policy).
   */
  onSubagentStart?: () => Promise<void> | void;
  /**
   * Invoked when a subagent ends or the subagent TTL expires without refresh.
   * Channel-specific handler — typically removes the subagentReaction.
   */
  onSubagentEnd?: () => Promise<void> | void;
};

export function createTypingCallbacks(params: CreateTypingCallbacksParams): TypingCallbacks {
  const stop = params.stop;
  const keepaliveIntervalMs = params.keepaliveIntervalMs ?? 3_000;
  const maxConsecutiveFailures = Math.max(1, params.maxConsecutiveFailures ?? 2);
  const maxDurationMs = params.maxDurationMs ?? 60_000; // Default 60s TTL
  let stopSent = false;
  let closed = false;
  let ttlTimer: ReturnType<typeof setTimeout> | undefined;

  const startGuard = createTypingStartGuard({
    isSealed: () => closed,
    onStartError: params.onStartError,
    maxConsecutiveFailures,
    onTrip: () => {
      keepaliveLoop.stop();
    },
  });

  const fireStart = async (): Promise<void> => {
    await startGuard.run(() => params.start());
  };

  const keepaliveLoop = createTypingKeepaliveLoop({
    intervalMs: keepaliveIntervalMs,
    onTick: fireStart,
  });

  // TTL safety: auto-stop typing after maxDurationMs
  const startTtlTimer = () => {
    if (maxDurationMs <= 0) {
      return;
    }
    clearTtlTimer();
    ttlTimer = setTimeout(() => {
      if (!closed) {
        console.warn(`[typing] TTL exceeded (${maxDurationMs}ms), auto-stopping typing indicator`);
        fireStop();
      }
    }, maxDurationMs);
  };

  const clearTtlTimer = () => {
    if (ttlTimer) {
      clearTimeout(ttlTimer);
      ttlTimer = undefined;
    }
  };

  const onReplyStart = async () => {
    if (closed) {
      return;
    }
    stopSent = false;
    startGuard.reset();
    keepaliveLoop.stop();
    clearTtlTimer();
    await fireStart();
    if (startGuard.isTripped()) {
      return;
    }
    keepaliveLoop.start();
    startTtlTimer(); // Start TTL safety timer
  };

  const fireStop = () => {
    closed = true;
    keepaliveLoop.stop();
    clearTtlTimer(); // Clear TTL timer on normal stop
    if (!stop || stopSent) {
      return;
    }
    stopSent = true;
    void stop().catch((err) => (params.onStopError ?? params.onStartError)(err));
  };

  const onRunSuccess = params.onRunSuccess
    ? () => {
        void Promise.resolve(params.onRunSuccess!()).catch((err) =>
          (params.onStopError ?? params.onStartError)(err),
        );
      }
    : undefined;

  const onRunFailure = params.onRunFailure
    ? (reason?: string) => {
        void Promise.resolve(params.onRunFailure!(reason)).catch((err) =>
          (params.onStopError ?? params.onStartError)(err),
        );
      }
    : undefined;

  const onSubagentStart = params.onSubagentStart
    ? () => {
        void Promise.resolve(params.onSubagentStart!()).catch((err) =>
          (params.onStopError ?? params.onStartError)(err),
        );
      }
    : undefined;

  const onSubagentEnd = params.onSubagentEnd
    ? () => {
        void Promise.resolve(params.onSubagentEnd!()).catch((err) =>
          (params.onStopError ?? params.onStartError)(err),
        );
      }
    : undefined;

  return {
    onReplyStart,
    onIdle: fireStop,
    onCleanup: fireStop,
    onRunSuccess,
    onRunFailure,
    onSubagentStart,
    onSubagentEnd,
  };
}
