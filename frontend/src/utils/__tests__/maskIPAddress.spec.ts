import { describe, expect, it } from 'vitest'
import { maskIPAddress } from '../formatters'

describe('maskIPAddress', () => {
  it('keeps only the first and last IPv4 octets', () => {
    expect(maskIPAddress('104.223.77.152')).toBe('104.***.***.152')
    expect(maskIPAddress(' 51.81.109.154 ')).toBe('51.***.***.154')
  })

  it('does not reveal IPv6 or malformed values', () => {
    expect(maskIPAddress('2001:db8::1')).toBe('****:****:****:****')
    expect(maskIPAddress('not-an-ip')).toBe('***')
    expect(maskIPAddress(null)).toBe('')
  })

  it('does not mask an IPv4 display value twice', () => {
    expect(maskIPAddress('67.***.***.47')).toBe('67.***.***.47')
  })
})
