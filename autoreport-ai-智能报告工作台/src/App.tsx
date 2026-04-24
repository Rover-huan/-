import {SmartAnalystApp} from './features/smart-analyst-app';
import {AdminApp} from './features/admin/admin-app';

export default function App() {
  if (typeof window !== 'undefined' && window.location.pathname.startsWith('/admin')) {
    return <AdminApp />;
  }
  return <SmartAnalystApp />;
}
