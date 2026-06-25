import type { Page } from '@playwright/test'
import { expect, test } from '@playwright/test'

type Backend = 'redis' | 'redb'

interface ResultSnapshot {
  r1: string | null
  r2: string | null
  r3: string | null
  totals: string | null
}

async function gotoCachePage(page: Page, params: { backend?: Backend | string, case?: string }) {
  const search = new URLSearchParams()
  if (params.backend !== undefined)
    search.set('backend', params.backend)
  if (params.case !== undefined)
    search.set('case', params.case)
  const qs = search.toString()
  await page.goto(`/use-cache-remote${qs ? `?${qs}` : ''}`)
}

async function readCachePage(page: Page): Promise<ResultSnapshot> {
  const [r1, r2, r3, totals] = await Promise.all([
    page.locator('[data-testid="result1"]').textContent(),
    page.locator('[data-testid="result2"]').textContent(),
    page.locator('[data-testid="result3"]').textContent(),
    page.locator('[data-testid="totals"]').textContent(),
  ])
  return { r1, r2, r3, totals: totals?.replace(/\s+/g, ' ') ?? null }
}

function uniqueCase(testInfo: { project: { name: string } }, label: string): string {
  return `${label}-${testInfo.project.name.replace(/\W+/g, '-')}-${Date.now()}`
}

test.describe('TestCacheHandler — use cache: remote', () => {
  test('identical args in one request return identical results, second label is independent', async ({ page }, testInfo) => {
    await gotoCachePage(page, { backend: 'redb', case: uniqueCase(testInfo, 'same-request') })
    const snap = await readCachePage(page)

    expect(snap.r1).toBe('first')
    expect(snap.r2).toBe('first')
    expect(snap.r1).toBe(snap.r2)
    expect(snap.r3).toBe('second')
    expect(snap.r3).not.toBe(snap.r1)

    expect(snap.totals).toContain('calls: 2')
  })

  test('cross-request cache hit on redb backend preserves result and totals', async ({ page }, testInfo) => {
    const cacheCase = uniqueCase(testInfo, 'cross-redb')

    await gotoCachePage(page, { backend: 'redb', case: cacheCase })
    const first = await readCachePage(page)
    expect(first.totals).toContain('calls: 2')

    await gotoCachePage(page, { backend: 'redb', case: cacheCase })
    const second = await readCachePage(page)
    expect(second.r1).toBe(first.r1)
    expect(second.r2).toBe(first.r2)
    expect(second.r3).toBe(first.r3)
    expect(second.totals).toBe(first.totals)
  })

  test('cross-request cache hit on redis backend preserves result and totals', async ({ page }, testInfo) => {
    const cacheCase = uniqueCase(testInfo, 'cross-redis')

    await gotoCachePage(page, { backend: 'redis', case: cacheCase })
    const first = await readCachePage(page)
    expect(first.totals).toContain('calls: 2')

    await gotoCachePage(page, { backend: 'redis', case: cacheCase })
    const second = await readCachePage(page)
    expect(second.r1).toBe(first.r1)
    expect(second.r2).toBe(first.r2)
    expect(second.r3).toBe(first.r3)
    expect(second.totals).toBe(first.totals)
  })

  test('different cache cases produce independent cache entries (redb)', async ({ page }, testInfo) => {
    const caseA = uniqueCase(testInfo, 'iso-A')
    const caseB = uniqueCase(testInfo, 'iso-B')

    await gotoCachePage(page, { backend: 'redb', case: caseA })
    const aFirst = await readCachePage(page)
    expect(aFirst.totals).toContain('calls: 2')

    await gotoCachePage(page, { backend: 'redb', case: caseB })
    const bFirst = await readCachePage(page)
    expect(bFirst.totals).toContain('calls: 2')

    await gotoCachePage(page, { backend: 'redb', case: caseA })
    const aSecond = await readCachePage(page)
    expect(aSecond.totals).toBe(aFirst.totals)
    expect(aSecond.r1).toBe(aFirst.r1)
    expect(aSecond.r3).toBe(aFirst.r3)

    await gotoCachePage(page, { backend: 'redb', case: caseB })
    const bSecond = await readCachePage(page)
    expect(bSecond.totals).toBe(bFirst.totals)
    expect(bSecond.r1).toBe(bFirst.r1)
    expect(bSecond.r3).toBe(bFirst.r3)
  })

  test('switching backend mid-test (redb → redis) preserves cached values for the same case', async ({ page }, testInfo) => {
    const cacheCase = uniqueCase(testInfo, 'backend-swap')

    await gotoCachePage(page, { backend: 'redb', case: cacheCase })
    const redbFirst = await readCachePage(page)
    expect(redbFirst.totals).toContain('calls: 2')

    await gotoCachePage(page, { backend: 'redis', case: cacheCase })
    const redisFirst = await readCachePage(page)
    expect(redisFirst.r1).toBe(redbFirst.r1)
    expect(redisFirst.r2).toBe(redbFirst.r2)
    expect(redisFirst.r3).toBe(redbFirst.r3)

    await gotoCachePage(page, { backend: 'redis', case: cacheCase })
    const redisSecond = await readCachePage(page)
    expect(redisSecond.totals).toBe(redisFirst.totals)
  })

  test('first request always counts 2 calls regardless of backend choice', async ({ page }, testInfo) => {
    for (const backend of ['redb', 'redis'] as const) {
      const cacheCase = uniqueCase(testInfo, `totals-${backend}`)
      await gotoCachePage(page, { backend, case: cacheCase })
      const snap = await readCachePage(page)
      expect(snap.totals, `first request to ${backend} must record exactly 2 calls`).toContain('calls: 2')
    }
  })
})
