// Minimal event emitter : `on` returns the function that unsubscribes. A handler that throws does
// not break the others nor the caller — the app's bug is logged, the room goes on.
export class Emitter {
  #handlers = Object.create(null);

  on(type, fn) {
    if (typeof fn !== 'function') throw new TypeError('listener must be a function');
    (this.#handlers[type] ??= []).push(fn);
    return () => {
      const list = this.#handlers[type];
      if (!list) return;
      const i = list.indexOf(fn);
      if (i >= 0) list.splice(i, 1);
    };
  }

  emit(type, ...args) {
    const list = this.#handlers[type];
    if (!list) return;
    for (const fn of list.slice()) {
      try { fn(...args); } catch (e) { console.error(`[signal] listener for '${type}' threw :`, e); }
    }
  }
}
