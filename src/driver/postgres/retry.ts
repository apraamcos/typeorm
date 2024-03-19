export async function retry<T = any, E = any>(
  func: (currentAttempt: number) => Promise<T>,
  validateError: (err: E, currentRetry: number) => Promise<boolean> | boolean,
  maxRetry = 5,
  retryCount = 0
): Promise<T> {
  const currentRetry = retryCount + 1;
  try {
    return await func(currentRetry);
  } catch (err: any) {
    const shouldRetry = await validateError(err as E, currentRetry);

    if (shouldRetry && maxRetry > 1) {
      // console.warn("retrying...", err);
      if (currentRetry > maxRetry) {
        console.warn(`max retry (${maxRetry}) hit ...`);
        throw err;
      }
      return await retry(func, validateError, maxRetry, currentRetry);
    }

    throw err;
  }
}
