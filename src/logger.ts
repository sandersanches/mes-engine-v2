function isDev(): boolean {
  return process.env.NODE_ENV !== 'production';
}

export const logger = {
  debug: (...args: unknown[]): void => {
    if (isDev()) {
      console.log(...args);
    }
  },
  info: (...args: unknown[]): void => {
    console.log(...args);
  },
  warn: (...args: unknown[]): void => {
    console.warn(...args);
  },
  error: (...args: unknown[]): void => {
    console.error(...args);
  },
};
