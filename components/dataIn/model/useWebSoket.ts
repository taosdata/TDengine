import { useWebSocket } from '@vueuse/core';
import { ref, watch } from 'vue';

export class WebSocketManager {
  private static instance: WebSocketManager;
  private connection: {
    send: AnyFunction;
    data: Ref<any>;
    status: Ref<string>;
    close: () => void;
  } | null = null;

  public status = ref<string>('CLOSED');

  private constructor() {}

  public static getInstance(): WebSocketManager {
    if (!WebSocketManager.instance) {
      WebSocketManager.instance = new WebSocketManager();
    }
    return WebSocketManager.instance;
  }

  public connect = (url: string, options?: any) => {
    if (!this.connection) {
      const { send, data, status, close } = useWebSocket(url, options);
      this.connection = { send, data, status, close };
      this.status.value = status.value;
      console.log('open: 建立连接', url);
    }
    return this.connection;
  };

  public getStatus() {
    return this.status;
  }

  public close = () => {
    if (this.connection) {
      this.connection?.close();
      this.connection = null;
      this.status.value = 'CLOSED';
      console.log('close: 关闭连接');
    }
  };
}

export interface ActivitieProps {
  activity: string;
  at: string;
  context: null | Recordable | string;
  id: string | number;
  level: string;
  status: string;
}

export function useActivitySubscription(url: string, message?: Recordable[]) {
  // WebSocket 状态和消息
  const wsManager = WebSocketManager.getInstance();
  const { send, data, status } = wsManager.connect(url, {
    heartbeat: {
      message: 'ping',
      interval: 2000,
      pongTimeout: 2000
    }
  });

  const error = ref<string | null>(null);
  const activity = ref<ActivitieProps>({} as ActivitieProps);

  // 发送消息到服务器，订阅任务数据
  const subscribe = () => {
    send(JSON.stringify(message));
  };

  // 监听接收到的消息
  watch(
    data,
    newMessages => {
      try {
        const parsed = JSON.parse(newMessages as string);
        if (parsed?.status == 'failed') {
          parsed.context = parsed.context?.message;
        }
        if (typeof parsed?.context == 'object') {
          parsed.context = null;
        }
        activity.value = parsed;
      } catch (err) {
        console.error('接收到的消息解析失败:', err);
      }
    },
    {
      deep: true,
      immediate: true
    }
  );

  // 返回数据和方法
  return {
    activity,
    error,
    subscribe,
    status,
    close: wsManager.close
  };
}
