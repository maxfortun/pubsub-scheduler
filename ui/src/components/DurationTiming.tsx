import type { CreateJobRequest } from '../types';

interface DurationTimingProps {
  form: CreateJobRequest;
  onChange: (updates: Partial<CreateJobRequest>) => void;
}

export function DurationTiming({ form, onChange }: DurationTimingProps) {
  const handleEndDateChange = (value: string) => {
    if (value) {
      onChange({ waitUntil: new Date(value).toISOString(), waitRepeat: 0 });
    } else {
      onChange({ waitUntil: undefined });
    }
  };

  const waitUntilLocal = form.waitUntil
    ? new Date(form.waitUntil).toISOString().slice(0, 16)
    : '';

  return (
    <div className="timing-section">
      <div className="form-group">
        <label>
          Duration{' '}
          <a
            href="https://en.wikipedia.org/wiki/ISO_8601#Durations"
            target="_blank"
            rel="noopener noreferrer"
            className="help-link"
            title="ISO 8601 Duration syntax"
          >
            (syntax)
          </a>
        </label>
        <input
          type="text"
          value={form.waitDuration || ''}
          onChange={e => onChange({ waitDuration: e.target.value || undefined })}
          placeholder="PT30S, PT5M, PT1H, P1D"
        />
        <div className="field-hint">
          Examples: PT30S (30 sec), PT5M (5 min), PT1H30M (1.5 hrs), P1D (1 day)
        </div>
      </div>
      <div className="form-row">
        <div className="form-group">
          <label>Repeat Count</label>
          <input
            type="number"
            value={form.waitRepeat ?? ''}
            onChange={e => onChange({
              waitRepeat: e.target.value ? parseInt(e.target.value) : undefined,
              waitUntil: undefined
            })}
            placeholder="1"
            min="0"
            disabled={!!form.waitUntil}
          />
          <div className="field-hint">1 = once, 0 = infinite</div>
        </div>
        <div className="form-group">
          <label>Until (optional)</label>
          <input
            type="datetime-local"
            value={waitUntilLocal}
            onChange={e => handleEndDateChange(e.target.value)}
            placeholder="Unlimited"
          />
          <div className="field-hint">Empty = unlimited</div>
        </div>
      </div>
    </div>
  );
}
