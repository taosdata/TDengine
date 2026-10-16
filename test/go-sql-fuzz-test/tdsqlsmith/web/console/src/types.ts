/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
