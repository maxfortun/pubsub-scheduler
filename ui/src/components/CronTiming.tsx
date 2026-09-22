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
          <label>Until</label>
          <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
            <div style={{ flex: 1, position: 'relative' }}>
              {!form.cronUntil && (
                <input
                  type="text"
                  value="Unlimited"
                  readOnly
                  onClick={(e) => {
                    const input = e.currentTarget.nextElementSibling as HTMLInputElement;
                    input?.showPicker?.();
                    input?.focus();
                  }}
                  style={{
                    width: '100%',
                    color: '#888',
                    cursor: 'pointer',
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    background: '#16213e'
                  }}
                />
              )}
              <input
                type="datetime-local"
                value={cronUntilLocal}
                onChange={e => handleUntilChange(e.target.value)}
                style={{
                  width: '100%',
                  opacity: form.cronUntil ? 1 : 0
                }}
              />
            </div>
            {form.cronUntil && (
              <button
                type="button"
                onClick={() => onChange({ cronUntil: undefined })}
                style={{
                  background: '#e74c3c',
                  border: 'none',
                  color: 'white',
                  padding: '6px 10px',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontSize: '12px'
                }}
              >
                Clear
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
