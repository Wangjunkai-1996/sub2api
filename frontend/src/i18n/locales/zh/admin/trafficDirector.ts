export default {
  trafficDirector: {
    title: '流量调度',
    description: '为 OpenAI 分组配置明确的权重池和备用链，控制请求导流边界。',
    group: 'OpenAI 分组',
    selectGroup: '选择 OpenAI 分组',
    selectGroupHint: '选择一个 OpenAI 分组，查看或发布它的导流策略。',
    noOpenAIGroups: '暂无可用的 OpenAI 分组。',
    modes: {
      legacy: 'Legacy',
      shadow: 'Shadow',
      enforced: 'Enforced'
    },
    healthModes: {
      off: '关闭',
      observe: '观察',
      enforce: '执行过滤'
    },
    status: {
      mode: '当前模式',
      version: '策略版本',
      accounts: '已分配账号',
      available: '可用账号',
      health: '健康模式',
      checksum: 'Checksum',
      summaryLabel: '流量调度状态摘要',
      poolLabel: '流量调度 Pool 状态'
    },
    editor: {
      title: '策略编辑器',
      hint: '只有发布 Enforced 版本后，Pool 边界才会真正限制请求。',
      mode: '策略模式',
      legacyHint: 'Legacy 完全保持现有调度路径。建议先用 Shadow 对比 Pool 决策，再逐步执行。',
      healthMode: '健康过滤',
      healthHint: '观察模式只记录健康变化；执行模式会从 Pool 候选中移除已熔断账号。',
      pools: 'Pool 列表',
      addPool: '新增 Pool',
      noPools: '请至少新增一个 Pool 来配置导流。',
      poolKey: 'Pool Key',
      weight: '权重 (bps)',
      minAvailable: '最少可用数',
      fallback: '备用 Pool',
      noFallback: '无备用',
      removePool: '删除 Pool',
      accounts: '账号',
      selected: '已选',
      ready: '可用',
      unavailable: '不可用',
      weightTotal: '正常权重合计：{total} / 10000',
      unassigned: '有 {count} 个账号未分配',
      unassignedHint: '未分配账号不会参与新导流。Preview 会列出这些账号；Enforced 发布时必须显式确认。',
      note: '发布备注',
      notePlaceholder: '说明本次策略变更原因',
      confirmUnassigned: '我确认未分配账号将被排除'
    },
    actions: {
      preview: 'Preview',
      publish: '发布'
    },
    preview: {
      title: 'Preview 结果',
      unassigned: '仍有 {count} 个账号在策略之外。'
    },
    history: {
      title: '版本历史',
      hint: '回滚会发布一个新版本，不会原地修改历史记录。',
      version: '版本',
      mode: '模式',
      createdAt: '创建时间',
      note: '备注',
      actions: '操作',
      rollback: '回滚',
      refresh: '刷新版本历史',
      rollbackLegacy: '回到 Legacy / v0',
      rollbackConfirm: '确定将版本 {version} 发布为新的当前策略吗？',
      confirmUnassigned: '我确认当前分组中未分配账号将继续被排除',
      empty: '暂无策略版本。'
    },
    messages: {
      published: '流量策略已发布。',
      rolledBack: '已通过新版本完成流量策略回滚。'
    },
    errors: {
      loadGroups: '加载 OpenAI 分组失败。',
      loadGroup: '加载流量策略失败。',
      loadVersions: '加载策略历史失败。',
      preview: '策略 Preview 失败。',
      publish: '策略发布失败。',
      rollback: '策略回滚失败。'
    }
  }
}
