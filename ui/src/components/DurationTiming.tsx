import type { CreateJobRequest } from '../types';

interface DurationTimingProps {
  form: CreateJobRequest;
  onChange: (updates: Partial<CreateJobRequest>) => void;
}

export function DurationTiming({ form, onChange }: DurationTimingProps) {
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
          value={form.sleepDuration || ''}
          onChange={e => onChange({ sleepDuration: e.target.value || undefined })}
          placeholder="PT30S, PT5M, PT1H, P1D"
        />
        <div className="field-hint">
          Examples: PT30S (30 sec), PT5M (5 min), PT1H30M (1.5 hrs), P1D (1 day)
        </div>
      </div>
      <div className="form-group">
        <label>Repeat Count</label>
        <input
          type="number"
          value={form.sleepRepeat ?? ''}
          onChange={e => onChange({ sleepRepeat: e.target.value ? parseInt(e.target.value) : undefined })}
          placeholder="1"
          min="0"
        />
        <div className="field-hint">1 = once, 0 = infinite</div>
      </div>
    </div>
  );
}
