import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient } from '@angular/common/http';
import { Subject, Subscription, timer } from 'rxjs';
import { switchMap, takeUntil } from 'rxjs/operators';
import { SparklineComponent } from './sparkline.component';
import { Metrics, Health, DlqEntry, PublishResponse, TopicRow } from './metrics.model';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule, SparklineComponent],
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css'],
})
export class AppComponent implements OnInit, OnDestroy {
  metrics: Metrics = emptyMetrics();
  health: Health = { status: 'DOWN', topics: [], queueDepths: {} };
  rows: TopicRow[] = [];

  autoRefresh = true;
  refreshMs = 2000;
  lastUpdated: Date | null = null;

  depthHistory = new Map<string, number[]>();
  publishedHistory = new Map<string, number[]>();

  dlqEntries = new Map<string, DlqEntry[]>();
  dlqOpen = new Map<string, boolean>();
  dlqBusy = new Map<string, boolean>();

  publishTopic = 'orders';
  publishPayload = '';
  publishMaxRetries = 3;
  publishTtlMillis = 0;
  lastPublishId = '';
  publishError = '';

  private stop$ = new Subject<void>();
  private pollSub: Subscription | null = null;

  constructor(private http: HttpClient) {}

  ngOnInit(): void {
    this.refresh();
    this.startPolling();
  }

  ngOnDestroy(): void {
    this.stop$.next();
    this.stop$.complete();
  }

  onRefreshMsChange(): void {
    this.restartPolling();
  }

  onAutoRefreshChange(): void {
    if (this.autoRefresh) {
      this.startPolling();
    } else {
      this.stopPolling();
    }
  }

  refresh(): void {
    this.http
      .get<Metrics>('/metrics')
      .pipe(takeUntil(this.stop$))
      .subscribe({
        next: (m) => {
          this.metrics = m;
          this.lastUpdated = new Date();
          this.computeRows();
        },
        error: () => (this.health = { status: 'DOWN', topics: [], queueDepths: {} }),
      });

    this.http
      .get<Health>('/health')
      .pipe(takeUntil(this.stop$))
      .subscribe({ next: (h) => (this.health = h) });
  }

  private startPolling(): void {
    this.stopPolling();
    this.pollSub = timer(this.refreshMs, this.refreshMs)
      .pipe(switchMap(() => this.http.get<Metrics>('/metrics')))
      .subscribe({
        next: (m) => {
          this.metrics = m;
          this.lastUpdated = new Date();
          this.computeRows();
        },
        error: () => (this.health = { status: 'DOWN', topics: [], queueDepths: {} }),
      });
  }

  private stopPolling(): void {
    this.pollSub?.unsubscribe();
    this.pollSub = null;
  }

  private restartPolling(): void {
    if (this.autoRefresh) {
      this.startPolling();
    }
  }

  private computeRows(): void {
    const topics = new Set<string>([
      ...Object.keys(this.metrics.queueDepths),
      ...Object.keys(this.metrics.published),
      ...Object.keys(this.metrics.acknowledged),
      ...Object.keys(this.metrics.deadLettered),
      ...Object.keys(this.metrics.retried),
      ...Object.keys(this.metrics.expired),
      ...Object.keys(this.metrics.rejected),
      ...this.health.topics,
    ]);

    this.rows = [...topics]
      .sort()
      .map((topic) => {
        const row: TopicRow = {
          topic,
          depth: num(this.metrics.queueDepths[topic]),
          published: num(this.metrics.published[topic]),
          acknowledged: num(this.metrics.acknowledged[topic]),
          deadLettered: num(this.metrics.deadLettered[topic]),
          retried: num(this.metrics.retried[topic]),
          expired: num(this.metrics.expired[topic]),
          rejected: num(this.metrics.rejected[topic]),
        };

        const depthHistory = this.depthHistory.get(topic) ?? [];
        depthHistory.push(row.depth);
        this.depthHistory.set(topic, depthHistory.slice(-60));

        const publishedHistory = this.publishedHistory.get(topic) ?? [];
        publishedHistory.push(row.published);
        this.publishedHistory.set(topic, publishedHistory.slice(-60));

        return row;
      });
  }

  totals(): { published: number; acked: number; inQueue: number; deadLettered: number; retried: number; expired: number } {
    return {
      published: this.rows.reduce((s, r) => s + r.published, 0),
      acked: this.rows.reduce((s, r) => s + r.acknowledged, 0),
      inQueue: this.rows.reduce((s, r) => s + r.depth, 0),
      deadLettered: this.rows.reduce((s, r) => s + r.deadLettered, 0),
      retried: this.rows.reduce((s, r) => s + r.retried, 0),
      expired: this.rows.reduce((s, r) => s + r.expired, 0),
    };
  }

  depthSparkline(topic: string): number[] {
    return this.depthHistory.get(topic) ?? [];
  }

  publishedSparkline(topic: string): number[] {
    return this.publishedHistory.get(topic) ?? [];
  }

  publish(): void {
    this.publishError = '';
    this.lastPublishId = '';
    const body: Record<string, unknown> = { payload: this.publishPayload };
    if (this.publishMaxRetries > 0) {
      body['maxRetries'] = this.publishMaxRetries;
    }
    if (this.publishTtlMillis > 0) {
      body['ttlMillis'] = this.publishTtlMillis;
    }
    this.http
      .post<PublishResponse>(`/publish/${encodeURIComponent(this.publishTopic)}`, body)
      .subscribe({
        next: (res) => (this.lastPublishId = res.messageId),
        error: (e) => (this.publishError = e?.error?.message ?? 'publish failed'),
      });
  }

  toggleDlq(topic: string): void {
    const open = !this.dlqOpen.get(topic);
    this.dlqOpen.set(topic, open);
    if (open) {
      this.loadDlq(topic);
    }
  }

  loadDlq(topic: string): void {
    this.http
      .get<DlqEntry[]>(`/dlq/${encodeURIComponent(topic)}`)
      .subscribe({ next: (list) => this.dlqEntries.set(topic, list) });
  }

  replayDlq(topic: string): void {
    this.dlqBusy.set(topic, true);
    this.http
      .post<{ replayed: number }>(`/dlq/${encodeURIComponent(topic)}/replay`, {})
      .subscribe({
        next: () => {
          this.dlqBusy.set(topic, false);
          this.loadDlq(topic);
          this.refresh();
        },
        error: () => this.dlqBusy.set(topic, false),
      });
  }

  decodePayload(base64: string): string {
    try {
      return decodeURIComponent(escape(atob(base64)));
    } catch {
      try {
        return atob(base64);
      } catch {
        return '(binary payload)';
      }
    }
  }

  dlqPreview(base64: string): string {
    const decoded = this.decodePayload(base64);
    return decoded.length > 120 ? decoded.slice(0, 120) + '…' : decoded;
  }
}

function num(v: number | undefined): number {
  return v ?? 0;
}

function emptyMetrics(): Metrics {
  return {
    queueDepths: {},
    published: {},
    acknowledged: {},
    deadLettered: {},
    retried: {},
    expired: {},
    rejected: {},
  };
}
