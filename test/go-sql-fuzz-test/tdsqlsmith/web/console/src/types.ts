export interface ReportSummary {
  run_id: string
  started_at: string
  generated_at: string
  execution_duration_ms: number
  completed: boolean
  incident_count: number
  taosd_incident_count: number
  tdsqlsmith_incident_count: number
  total_executed: number
  query_rule_hit: number
  query_rule_required: number
  query_rule_coverage_ratio: number
  query_rule_missing_count: number
}

export interface CrashIncident {
  incident_id?: string
  occurred_at: string
  crash_sql?: string
  sql?: string
  candidate_sql?: string
}

export interface QueryRuleCoverage {
  required: number
  hit: number
  missing: string[]
  coverage_ratio: number
}

export interface QueryRuleProgressPoint {
  query_no: number
  hit: number
  required: number
  missing: number
  coverage_ratio: number
  top_missing?: string[]
}

export interface ReportDetail {
  run_id: string
  started_at: string
  generated_at: string
  execution_duration_ms: number
  completed: boolean
  setup_sql?: string[]
  total_executed: number
  query_rule_coverage: QueryRuleCoverage
  query_rule_progress?: QueryRuleProgressPoint[]
  query_combo_counts?: Record<string, number>
  taosd_incidents?: CrashIncident[]
  tdsqlsmith_incidents?: CrashIncident[]
}
