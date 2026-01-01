import { PageHeader } from '@/components/common/PageHeader';
import { useMessages, useNavigation } from '@/components/hooks';
import { WebsiteAddButton } from './WebsiteAddButton';

export interface WebsitesHeaderProps {
  allowCreate?: boolean;
}

export function WebsitesHeader({ allowCreate = true }: WebsitesHeaderProps) {
  const { formatMessage, labels } = useMessages();

  return (
    <PageHeader title={formatMessage(labels.websites)}>
      {allowCreate && <WebsiteAddButton />}
    </PageHeader>
  );
}
