export default {
  trafficDirector: {
    title: 'Traffic Director',
    description: 'Route OpenAI group traffic through explicit weighted pools and fallback chains.',
    group: 'OpenAI group',
    selectGroup: 'Select an OpenAI group',
    selectGroupHint: 'Choose an OpenAI group to review or publish its traffic policy.',
    noOpenAIGroups: 'No OpenAI groups are available.',
    modes: {
      legacy: 'Legacy',
      shadow: 'Shadow',
      enforced: 'Enforced'
    },
    healthModes: {
      off: 'Off',
      observe: 'Observe',
      enforce: 'Enforce'
    },
    status: {
      mode: 'Current mode',
      version: 'Policy version',
      accounts: 'Assigned accounts',
      available: 'Schedulable accounts',
      health: 'Health mode',
      checksum: 'Checksum',
      summaryLabel: 'Traffic Director status summary',
      poolLabel: 'Traffic Director pool status'
    },
    editor: {
      title: 'Policy editor',
      hint: 'Pool boundaries are enforced only after an Enforced version is published.',
      mode: 'Policy mode',
      legacyHint: 'Legacy keeps the existing scheduler path unchanged. Use Shadow to compare pool decisions before enforcing.',
      healthMode: 'Health filtering',
      healthHint: 'Observe records health transitions; Enforce removes open accounts from pool candidates.',
      pools: 'Pools',
      addPool: 'Add pool',
      noPools: 'Add at least one pool to configure routing.',
      poolKey: 'Pool key',
      weight: 'Weight (bps)',
      minAvailable: 'Min available',
      fallback: 'Fallback pool',
      noFallback: 'No fallback',
      removePool: 'Remove pool',
      accounts: 'Accounts',
      selected: 'selected',
      ready: 'schedulable',
      unavailable: 'not schedulable',
      weightTotal: 'Positive weight total: {total} / 10000',
      unassigned: '{count} account(s) are unassigned',
      unassignedHint: 'Unassigned accounts never participate in Traffic Director routing. Preview lists them; Enforced publishing requires explicit confirmation.',
      note: 'Release note',
      notePlaceholder: 'Why is this policy being changed?',
      confirmUnassigned: 'I understand unassigned accounts will be excluded'
    },
    actions: {
      preview: 'Preview',
      publish: 'Publish'
    },
    preview: {
      title: 'Preview result',
      unassigned: '{count} account(s) will remain outside the policy.'
    },
    history: {
      title: 'Version history',
      hint: 'Rollback publishes a new version and never edits history in place.',
      version: 'Version',
      mode: 'Mode',
      createdAt: 'Created',
      note: 'Note',
      actions: 'Actions',
      rollback: 'Rollback',
      refresh: 'Refresh version history',
      loadMore: 'Load more ({remaining} remaining)',
      rollbackLegacy: 'Return to Legacy / v0',
      rollbackConfirm: 'Publish version {version} as the new current policy?',
      loadingDetails: 'Loading target policy...',
      targetMode: 'Target mode',
      targetChecksum: 'Target checksum',
      targetUnassigned: 'Unassigned account IDs after rollback',
      targetSpec: 'Target policy spec',
      none: 'None',
      confirmUnassigned: 'I confirm unassigned accounts in the current group remain excluded',
      empty: 'No policy versions yet.'
    },
    messages: {
      published: 'Traffic policy published.',
      rolledBack: 'Traffic policy rolled back as a new version.'
    },
    errors: {
      loadGroups: 'Unable to load OpenAI groups.',
      loadGroup: 'Unable to load the traffic policy.',
      loadVersions: 'Unable to load policy history.',
      loadVersion: 'Unable to load the target policy version.',
      preview: 'Policy preview failed.',
      publish: 'Policy publish failed.',
      rollback: 'Policy rollback failed.',
      versionConflict: 'The policy changed on the server. The latest version has been loaded; review it before retrying.',
      TRAFFIC_DIRECTOR_GROUP_NOT_FOUND: 'The selected OpenAI group no longer exists.',
      TRAFFIC_DIRECTOR_VERSION_NOT_FOUND: 'The selected policy version no longer exists.',
      TRAFFIC_DIRECTOR_VALIDATION_FAILED: 'The policy is invalid. Review the pool weights, account assignments, fallback chain, and confirmation before retrying.',
      TRAFFIC_DIRECTOR_VERSION_CONFLICT: 'The policy changed from version {expected_version} to {actual_version}. The latest version has been loaded; review it before retrying.',
      TRAFFIC_DIRECTOR_IDEMPOTENCY_CONFLICT: 'This operation key was already used for different policy data. Start the operation again.',
      TRAFFIC_DIRECTOR_POLICY_UNAVAILABLE: 'The current traffic policy is temporarily unavailable. Try again after the control plane recovers.',
      TRAFFIC_DIRECTOR_NO_AVAILABLE_POOL: 'No schedulable account is available in the configured pool and fallback chain.'
    }
  }
}
