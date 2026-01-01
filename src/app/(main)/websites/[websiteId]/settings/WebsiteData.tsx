import { Button, Column, Dialog, DialogTrigger, Modal } from '@umami/react-zen';
import { ActionForm } from '@/components/common/ActionForm';
import { useLoginQuery, useMessages, useModified, useNavigation } from '@/components/hooks';
import { WebsiteDeleteForm } from './WebsiteDeleteForm';
import { WebsiteResetForm } from './WebsiteResetForm';

export function WebsiteData({ websiteId, onSave }: { websiteId: string; onSave?: () => void }) {
  const { formatMessage, labels, messages } = useMessages();
  const { touch } = useModified();
  const { router, renderUrl } = useNavigation();

  const handleSave = () => {
    touch('websites');
    onSave?.();
    router.push(renderUrl(`/websites`));
  };

  const handleReset = async () => {
    onSave?.();
  };

  return (
    <Column gap="6">
      <ActionForm
        label={formatMessage(labels.resetWebsite)}
        description={formatMessage(messages.resetWebsiteWarning)}
      >
        <DialogTrigger>
          <Button>{formatMessage(labels.reset)}</Button>
          <Modal>
            <Dialog title={formatMessage(labels.resetWebsite)} style={{ width: 400 }}>
              {({ close }) => (
                <WebsiteResetForm websiteId={websiteId} onSave={handleReset} onClose={close} />
              )}
            </Dialog>
          </Modal>
        </DialogTrigger>
      </ActionForm>

      <ActionForm
        label={formatMessage(labels.deleteWebsite)}
        description={formatMessage(messages.deleteWebsiteWarning)}
      >
        <DialogTrigger>
          <Button data-test="button-delete" variant="danger">
            {formatMessage(labels.delete)}
          </Button>
          <Modal>
            <Dialog title={formatMessage(labels.deleteWebsite)} style={{ width: 400 }}>
              {({ close }) => (
                <WebsiteDeleteForm websiteId={websiteId} onSave={handleSave} onClose={close} />
              )}
            </Dialog>
          </Modal>
        </DialogTrigger>
      </ActionForm>
    </Column>
  );
}
