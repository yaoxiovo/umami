import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { useEffect, useState } from 'react';
import { buildPath } from '@/lib/url';

export function useNavigation() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [, websiteId] = pathname.match(/\/websites\/([a-f0-9-]+)/) || [];
  const [queryParams, setQueryParams] = useState(Object.fromEntries(searchParams));

  const updateParams = (params?: Record<string, string | number>) => {
    return buildPath(pathname, { ...queryParams, ...params });
  };

  const replaceParams = (params?: Record<string, string | number>) => {
    return buildPath(pathname, params);
  };

  const renderUrl = (path: string, params?: Record<string, string | number> | false) => {
    return buildPath(path, params === false ? {} : { ...queryParams, ...params });
  };

  useEffect(() => {
    setQueryParams(Object.fromEntries(searchParams));
  }, [searchParams.toString()]);

  return {
    router,
    pathname,
    searchParams,
    query: queryParams,
    websiteId,
    updateParams,
    replaceParams,
    renderUrl,
  };
}
