import type { CreateJobRequest } from '../types';

interface AtTimingProps {
  form: CreateJobRequest;
  onChange: (updates: Partial<CreateJobRequest>) => void;
}

export function AtTiming({ form, onChange }: AtTimingProps) {
  const handleDateChange = (value: string) => {
    if (value) {
      onChange({ fireAt: new Date(value).toISOString() });
    } else {
      onChange({ fireAt: undefined });
    }
  };

  const localValue = form.fireAt
    ? new Date(form.fireAt).toISOString().slice(0, 16)
    : '';

  return (
    <div className="timing-section">
      <div className="form-group">
        <label>Fire At (absolute time)</label>
        <input
          type="datetime-local"
          value={localValue}
          onChange={e => handleDateChange(e.target.value)}
        />
        <div className="field-hint">
          Job will fire at exactly this date and time
        </div>
      </div>
    </div>
  );
}
