import React, { ReactElement, useState } from 'react'
import { Collapse, InlineField, InlineFieldRow, TextArea } from '@grafana/ui'
import type { EditorProps } from './types'
import { useChangeOptionsArea } from './useChangeString'

export function QueryEditor(props: EditorProps): ReactElement {
    const { query } = props;
    if (!query.queryType) {
        query.queryType = "SQL"
    }

    const onChangeSql = useChangeOptionsArea(props, { propertyName: 'sql', runQuery: false })
    const onblurSql = useChangeOptionsArea(props, { propertyName: 'sql', runQuery: true })

    const ShowGeneratedSql = () => {
        const [isOpen, setIsOpen] = useState<boolean>(false)
        let sql = props.datasource.getGenerateSql()

        return (
            <Collapse
                label='Generate SQL'
                className='gf-form-label width-10'
                collapsible={true}
                isOpen={isOpen}
                onToggle={() =>
                    setIsOpen(!isOpen)
                }
            >
                <p>{sql}</p>
            </Collapse>
        )
    }
    const ShowHelpCollapse = () => {
        const [isOpen, setIsOpen] = useState<boolean>(false)
        let help = 'Use any SQL that can return Resultset such as:</br>' +
            '- [[timestamp1, value1], [timestamp2, value2], ... ]</br>' +
            '</br>' +
            'Macros:</br>' +
            '- $from -&gt; start timestamp of panel</br>' +
            '- $to -&gt; stop timestamp of panel</br>' +
            '- $interval -&gt; interval of panel</br>' +
            '- $__timeFrom() -&gt; same as $from</br>' +
            '- $__timeTo() -&gt; same as $to</br>' +
            '- $__timeFilter(ts_col) -&gt; ts_col &gt;= $from AND ts_col &lt;= $to</br>' +
            '</br>' +
            'Example of SQL:</br>' +
            '&nbsp;&nbsp;SELECT count(*)</br> FROM db.table WHERE ts > $from and ts < $to INTERVAL ($interval)'

        return (
            <Collapse
                label='Show Help'
                className='gf-form-label width-10'
                collapsible={true}
                isOpen={isOpen}
                onToggle={() =>
                    setIsOpen(!isOpen)
                }
            >
                <p dangerouslySetInnerHTML={{ __html: help }}></p>
            </Collapse>
        )
    }

    const deprecatedArithmetic = query.queryType && query.queryType !== 'SQL'

    return (
        <div className='gf-form-group'>
            {deprecatedArithmetic && (
                <InlineFieldRow>
                    <div className="gf-form-label width-20">Deprecated</div>
                    <div className="gf-form gf-form--grow">
                        Arithmetic queries are no longer supported here. Migrate this panel to Grafana Expressions/Math,
                        for example `$A + $B`.
                    </div>
                </InlineFieldRow>
            )}

            <InlineFieldRow>
                <InlineField label='Input Sql' labelWidth={20} tooltip='' grow>
                    <TextArea
                        style={{ width: "100%", minWidth: "800px" }}
                        rows={5}
                        className={'min-width-30 max-width-100 gf-form--grow'}
                        placeholder={'select _wstart as ts, avg(mem_free), dnode_ep from log.taosd_dnodes_info where _ts>=$from and _ts<=$to partition by dnode_ep interval($interval)'}
                        onChange={onChangeSql}
                        onBlur={onblurSql}
                        value={query.sql}
                    />
                </InlineField>
            </InlineFieldRow>

            <InlineFieldRow>
                <ShowGeneratedSql />
                <ShowHelpCollapse />
            </InlineFieldRow>
        </div>
    );
}
