import {
  Column,
  Icon,
  IconLabel,
  Menu,
  MenuItem,
  MenuSeparator,
  MenuTrigger,
  Popover,
  Pressable,
  Row,
  Text,
} from '@umami/react-zen';
import { ArrowRight } from 'lucide-react';
import type { Key } from 'react';
import { useConfig, useLoginQuery, useMessages, useMobile } from '@/components/hooks';
import {
  BookText,
  ChevronRight,
  ExternalLink,
  LifeBuoy,
  LockKeyhole,
  LogOut,
  Settings,
  User,
} from '@/components/icons';
import { DOCS_URL } from '@/lib/constants';

export interface TeamsButtonProps {
  showText?: boolean;
  onAction?: (id: any) => void;
}

export function NavButton({ showText = true }: TeamsButtonProps) {
  const { user } = useLoginQuery();
  const { cloudMode } = useConfig();
  const { formatMessage, labels } = useMessages();
  const { isMobile } = useMobile();
  const label = user.username;

  const getUrl = (url: string) => {
    return cloudMode ? `${process.env.cloudUrl}${url}` : url;
  };

  return (
    <MenuTrigger>
      <Pressable>
        <Row
          alignItems="center"
          justifyContent="space-between"
          flexGrow={1}
          padding
          border
          borderRadius
          shadow="1"
          maxHeight="40px"
          role="button"
          style={{ cursor: 'pointer', textWrap: 'nowrap', overflow: 'hidden', outline: 'none' }}
        >
          <Row alignItems="center" position="relative" gap maxHeight="40px">
            <Icon>
              <User />
            </Icon>
            {showText && <Text>{label}</Text>}
          </Row>
          {showText && (
            <Icon rotate={90} size="sm">
              <ChevronRight />
            </Icon>
          )}
        </Row>
      </Pressable>
      <Popover placement="bottom start">
        <Column minWidth="300px">
          <Menu autoFocus="last">
            <MenuItem
              id="settings"
              href={getUrl('/settings')}
              icon={<Settings />}
              label={formatMessage(labels.settings)}
            />
            {cloudMode && (
              <>
                <MenuItem
                  id="docs"
                  href={DOCS_URL}
                  target="_blank"
                  icon={<BookText />}
                  label={formatMessage(labels.documentation)}
                >
                  <Icon color="muted">
                    <ExternalLink />
                  </Icon>
                </MenuItem>
                <MenuItem
                  id="support"
                  href={getUrl('/settings/support')}
                  icon={<LifeBuoy />}
                  label={formatMessage(labels.support)}
                />
              </>
            )}
            {!cloudMode && user.isAdmin && (
              <>
                <MenuSeparator />
                <MenuItem
                  id="/admin"
                  href="/admin"
                  icon={<LockKeyhole />}
                  label={formatMessage(labels.admin)}
                />
              </>
            )}
            <MenuSeparator />
            <MenuItem
              id="logout"
              href={getUrl('/logout')}
              icon={<LogOut />}
              label={formatMessage(labels.logout)}
            />
          </Menu>
        </Column>
      </Popover>
    </MenuTrigger>
  );
}
