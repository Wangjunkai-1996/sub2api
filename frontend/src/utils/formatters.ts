/**
 * 格式化缓存 token 数量（1K/1M 缩写）
 */
export function formatCacheTokens(tokens: number): string {
  if (tokens >= 1000000) return `${(tokens / 1000000).toFixed(1)}M`
  if (tokens >= 1000) return `${(tokens / 1000).toFixed(1)}K`
  return tokens.toLocaleString()
}

/** Mask an IP for UI display without changing the underlying API value. */
export function maskIPAddress(value: string | null | undefined): string {
  const input = value?.trim()
  if (!input) return ''
  if (/^\d{1,3}\.\*{3}\.\*{3}\.\d{1,3}$/.test(input)) return input

  const octets = input.split('.')
  if (
    octets.length === 4
    && octets.every((octet) => /^\d{1,3}$/.test(octet) && Number(octet) <= 255)
  ) {
    return `${Number(octets[0])}.***.***.${Number(octets[3])}`
  }
  if (input.includes(':')) return '****:****:****:****'
  return '***'
}

/**
 * 自适应精度格式化倍率：保留至多 4 位小数并去掉末尾多余的 0，
 * 但至少保留 2 位小数（0.035 -> "0.035"，0.3 -> "0.30"，1 -> "1.00"）
 */
export function formatMultiplier(val: number): string {
  if (val < 0.0001) return val.toPrecision(2)
  return val.toFixed(4).replace(/(\.\d{2}\d*?)0+$/, '$1')
}
