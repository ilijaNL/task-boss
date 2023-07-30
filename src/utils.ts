import delay from 'delay';

export type Simplify<T> = { [K in keyof T]: T[K] } & {};

export type JsonArray = JsonValue[];

export type JsonObject = {
  [K in string]?: JsonValue;
};

export type JsonPrimitive = boolean | null | number | string;

export type JsonValue = JsonArray | JsonObject | JsonPrimitive;

export const debounce = <Args extends any[], F extends (...args: Args) => any>(
  fn: F,
  opts: { ms: number; maxMs: number }
) => {
  let timeoutId: ReturnType<typeof setTimeout>;
  let maxTimeoutId: ReturnType<typeof setTimeout>;
  let isFirstInvoke = true;
  let startedTime = Date.now();

  return function call(this: ThisParameterType<F>, ...args: Parameters<F>) {
    clearTimeout(timeoutId);
    clearTimeout(maxTimeoutId);

    const invoke = () => {
      clearTimeout(maxTimeoutId);
      clearTimeout(timeoutId);

      isFirstInvoke = true;

      fn.apply(this, args);
    };

    if (isFirstInvoke) {
      isFirstInvoke = false;
      startedTime = Date.now();
    }

    timeoutId = setTimeout(invoke, opts.ms);
    // need to reset every time otherwise it wont use the latest this & args in invoke
    maxTimeoutId = setTimeout(invoke, Math.max(0, opts.maxMs - (Date.now() - startedTime)));
  };
};

export const resolveWithinSeconds = async (promise: Promise<any>, seconds: number) => {
  const timeout = Math.max(0.01, seconds) * 1000;
  const timeoutReject = delay.reject(timeout, { value: new Error(`handler execution exceeded ${timeout}ms`) });

  let result;

  try {
    result = await Promise.race([promise, timeoutReject]);
  } finally {
    try {
      timeoutReject.clear();
    } catch {}
  }

  return result;
};

export function replaceErrors(value: any) {
  if (value instanceof Error) {
    var error = {} as any;

    Object.getOwnPropertyNames(value).forEach(function (propName) {
      error[propName] = (value as any)[propName];
    });

    return error;
  }

  return value;
}

export function mapCompletionDataArg(data: any) {
  if (data === null || typeof data === 'undefined' || typeof data === 'function') {
    return null;
  }

  const result = typeof data === 'object' && !Array.isArray(data) ? data : { value: data };

  return replaceErrors(result);
}

export class DeferredPromise<T = unknown> {
  private _resolve: null | ((value: T) => void) = null;
  private _reject: null | ((reason?: any) => void) = null;
  private _settled: boolean = false;
  public promise: Promise<T>;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this._reject = reject;
      this._resolve = resolve;
    });
  }

  public resolve(value: T) {
    if (this._settled) {
      return;
    }

    this._settled = true;
    this._resolve?.(value);
  }

  public reject(reason?: any) {
    if (this._settled) {
      return;
    }
    this._settled = true;
    this._reject?.(reason);
  }
}
