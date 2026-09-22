import type { CreateJobRequest } from '../types';

interface CronTimingProps {
  form: CreateJobRequest;
  onChange: (updates: Partial<CreateJobRequest>) => void;
}

export function CronTiming({ form, onChange }: CronTimingProps) {
  const handleUntilChange = (value: string) => {
    if (value) {
      onChange({ cronUntil: new Date(value).toISOString() });
    } else {
      onChange({ cronUntil: undefined });
    }
  };

  const cronUntilLocal = form.cronUntil
    ? new Date(form.cronUntil).toISOString().slice(0, 16)
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
          <label>Repeat Count</label>
          <input
            type="number"
            value={form.cronRepeat ?? ''}
            onChange={e => onChange({ cronRepeat: e.target.value ? parseInt(e.target.value) : undefined })}
            placeholder="unlimited"
            min="0"
          />
          <div className="field-hint">Empty = unlimited</div>
        </div>
        <div className="form-group">
          <label>Until (optional)</label>
          <div className="input-with-clear">
            <input
              type="datetime-local"
              value={cronUntilLocal}
              onChange={e => handleUntilChange(e.target.value)}
            />
            {form.cronUntil && (
              <button type="button" className="clear-input-btn" onClick={() => onChange({ cronUntil: undefined })}>×</button>
            )}
          </div>
          <div className="field-hint">Empty = unlimited</div>
        </div>
      </div>
    </div>
  );
}
