export interface Metrics {
  queueDepths: Record<string, number>;
  published: Record<string, number>;
  acknowledged: Record<string, number>;
  deadLettered: Record<string, number>;
  retried: Record<string, number>;
  expired: Record<string, number>;
  rejected: Record<string, number>;
}

export interface Health {
  status: string;
  topics: string[];
  queueDepths: Record<string, number>;
}

export interface DlqEntry {
  id: string;
  topic: string;
  status: string;
  deliveryAttempts: number;
  maxRetries: number;
  publishedAt: number;
  payload: string;
}

export interface PublishResponse {
  messageId: string;
}

export interface TopicRow {
  topic: string;
  depth: number;
  published: number;
  acknowledged: number;
  deadLettered: number;
  retried: number;
  expired: number;
  rejected: number;
}
