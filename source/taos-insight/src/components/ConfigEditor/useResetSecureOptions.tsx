import {useCallback} from 'react'
import type {SecureJsonData} from 'types'
import type {EditorProps} from './types'

export function useResetSecureOptions(props: EditorProps, propertyName: keyof SecureJsonData): () => void {
    const {onOptionsChange, options} = props;

    return useCallback(() => {
        onOptionsChange({
            ...options,
            secureJsonFields: {
                ...options.secureJsonFields,
                [propertyName]: false,
            },
            secureJsonData: {
                ...options.secureJsonData,
                [propertyName]: '',
            },
        });
    }, [onOptionsChange, options, propertyName]);
}
