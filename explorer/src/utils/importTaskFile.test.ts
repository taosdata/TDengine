import { describe, expect, it } from 'vitest';

import { rewriteKafkaImportContent } from '../../tests/_utils/importTaskFile';

describe('rewriteKafkaImportContent', () => {
  it('replaces the placeholder Kafka broker with the CI broker', () => {
    const rewritten = rewriteKafkaImportContent(
      JSON.stringify({
        tasks: [
          {
            from: {
              type: 'kafka',
              data: {
                host: 'broker.example.invalid',
                port: 9092,
                endpoint: 'broker.example.invalid:9092'
              }
            }
          }
        ]
      }),
      '192.168.1.45:19092'
    );

    expect(JSON.parse(rewritten)).toEqual({
      tasks: [
        {
          from: {
            type: 'kafka',
            data: {
              host: '192.168.1.45',
              port: 19092,
              endpoint: '192.168.1.45:19092'
            }
          }
        }
      ]
    });
  });

  it('keeps the original content when no CI broker is provided', () => {
    const original = JSON.stringify({
      tasks: [
        {
          from: {
            type: 'kafka',
            data: {
              host: 'broker.example.invalid',
              port: 9092,
              endpoint: 'broker.example.invalid:9092'
            }
          }
        }
      ]
    });

    expect(rewriteKafkaImportContent(original)).toBe(original);
  });
});
