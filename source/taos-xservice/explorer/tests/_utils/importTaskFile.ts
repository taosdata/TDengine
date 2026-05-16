function parseBroker(ciBroker: string) {
  const [host, portText] = ciBroker.split(':');
  const port = Number(portText);

  if (!host || !Number.isInteger(port) || port <= 0) {
    throw new Error(`invalid Kafka broker: ${ciBroker}`);
  }

  return { host, port, endpoint: `${host}:${port}` };
}

export function rewriteKafkaImportContent(contents: string, ciBroker?: string): string {
  if (!ciBroker) {
    return contents;
  }

  const parsed = JSON.parse(contents) as {
    tasks?: Array<{
      from?: {
        type?: string;
        data?: Record<string, unknown>;
      };
    }>;
  };
  const broker = parseBroker(ciBroker);

  parsed.tasks?.forEach(task => {
    if (task.from?.type !== 'kafka' || !task.from.data) {
      return;
    }

    task.from.data.host = broker.host;
    task.from.data.port = broker.port;
    task.from.data.endpoint = broker.endpoint;
  });

  return JSON.stringify(parsed);
}
