import type { CreateJobRequest } from '../types';

interface CronTimingProps {
  form: CreateJobRequest;
  onChange: (updates: Partial<CreateJobRequest>) => void;
}

export function CronTiming({ form, onChange }: CronTimingProps) {
  const handleEndDateChange = (value: string) => {
    if (value) {
      onChange({ cronEnd: new Date(value).toISOString() });
    } else {
      onChange({ cronEnd: undefined });
    }
  };

  const cronEndLocal = form.cronEnd
    ? new Date(form.cronEnd).toISOString().slice(0, 16)
    : '';

  return (
    <div className="timing-section">
      <div className="form-group">
        <label>
          Cron Expression{' '}
          <a
            href="https://en.wikipedia.org/wiki/Cron#CRON_expression"
            target="_blank"
            rel="noopener noreferrer"
            className="help-link"
            title="Cron expression syntax"
          >
            (syntax)
          </a>
        </label>
        <input
          type="text"
          value={form.cronExpression || ''}
          onChange={e => onChange({ cronExpression: e.target.value || undefined })}
          placeholder="0 * * * *"
        />
        <div className="field-hint">
          Format: min hr dom mon dow (e.g., "0 9 * * 1-5" = 9am weekdays)
        </div>
      </div>
      <div className="form-row">
        <div className="form-group">
          <label>Max Fire Count</label>
          <input
            type="number"
            value={form.cronMaxCount ?? ''}
            onChange={e => onChange({ cronMaxCount: e.target.value ? parseInt(e.target.value) : undefined })}
            placeholder="unlimited"
            min="0"
          />
          <div className="field-hint">0 or empty = unlimited</div>
        </div>
        <div className="form-group">
          <label>End Date (optional)</label>
          <input
            type="datetime-local"
            value={cronEndLocal}
            onChange={e => handleEndDateChange(e.target.value)}
          />
          <div className="field-hint">Stop scheduling after this time</div>
        </div>
      </div>
    </div>
  );
}
