import { ChangeEvent, useCallback } from 'react'
import type { ChangeOptions, Query } from '../../types'
import type { EditorProps } from './types'

export function useChangeOptions(props: EditorProps, options: ChangeOptions<Query>): (value: ChangeEvent<HTMLInputElement>) => void {
    const { onChange, onRunQuery, query } = props;
    const { propertyName, runQuery } = options;

    return useCallback(
        (value: ChangeEvent<HTMLInputElement>) => {
            if (!value) {
                return;
            }

            onChange({
                ...query,
                [propertyName]: value.target.value,
            });

            if (runQuery) {
                onRunQuery();
            }
        },
        [onChange, onRunQuery, query, propertyName, runQuery]
    );
}

// 修改 useChangeOptions 自定义 Hook
export function useChangeOptionsArea(props: EditorProps, options: ChangeOptions<any>): (value: ChangeEvent<HTMLTextAreaElement>) => void {
    const { onChange, onRunQuery, query } = props;
    const { propertyName, runQuery } = options;

    return useCallback(
        (value: ChangeEvent<HTMLTextAreaElement>) => {
            if (!value) {
                return;
            }

            onChange({
                ...query,
                [propertyName]: value.target.value,
            });

            if (runQuery) {
                onRunQuery();
            }
        },
        [onChange, onRunQuery, query, propertyName, runQuery]
    );
}
