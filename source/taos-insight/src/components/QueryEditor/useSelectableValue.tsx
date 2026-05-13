import {useMemo} from 'react'
import type {SelectableValue} from '@grafana/data'

export function useSelectableValue(value: string, label?: string): SelectableValue<string> | undefined {
    return useMemo(() => {
        if (!value) {
            return;
        }

        return {
            label: label ?? value,
            value: value,
        };
    }, [label, value]);
}
