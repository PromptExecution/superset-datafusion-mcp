/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { t } from '@apache-superset/core';
import { SupersetClient } from '@superset-ui/core';
import { css, styled } from '@apache-superset/core/ui';
import { useEffect, useState } from 'react';
import { ensureAppRoot } from 'src/utils/pathUtils';

import { KrokiChartProps, KrokiRenderApiResponse } from './types';

const Root = styled.div<{ height: number; width: number }>`
  ${({ height, width, theme }) => css`
    height: ${height}px;
    width: ${width}px;
    overflow: auto;
    border: 1px solid ${theme.colorBorder};
    border-radius: ${theme.borderRadius}px;
    padding: ${theme.sizeUnit * 2}px;
    background: ${theme.colorBgContainer};
  `}
`;

const Message = styled.div`
  ${({ theme }) => css`
    color: ${theme.colorTextSecondary};
  `}
`;

const ErrorMessage = styled.div`
  ${({ theme }) => css`
    color: ${theme.colorErrorText};
  `}
`;

export default function KrokiChart({
  width,
  height,
  formData,
}: KrokiChartProps) {
  const [svgMarkup, setSvgMarkup] = useState<string>('');
  const [error, setError] = useState<string>('');
  const [isLoading, setIsLoading] = useState<boolean>(false);

  const diagramType = formData.diagram_type || 'mermaid';
  const diagramSource = formData.diagram_source || '';

  useEffect(() => {
    if (!diagramSource.trim()) {
      setSvgMarkup('');
      setError(t('Diagram source is empty.'));
      return;
    }

    let isActive = true;
    const controller = new AbortController();

    const renderDiagram = async () => {
      setIsLoading(true);
      setError('');
      try {
        const { json } = await SupersetClient.post({
          endpoint: ensureAppRoot('/api/v1/kroki/render/'),
          jsonPayload: {
            diagram_type: diagramType,
            diagram_source: diagramSource,
            output_format: 'svg',
          },
          signal: controller.signal,
        });

        const apiResponse = json as KrokiRenderApiResponse;
        const svg = apiResponse.result?.svg || '';

        if (!svg.trim()) {
          throw new Error(t('Kroki renderer returned an empty SVG response.'));
        }

        if (isActive) {
          setSvgMarkup(svg);
          setError('');
        }
      } catch (caughtError) {
        if (controller.signal.aborted) {
          return;
        }
        if (isActive) {
          const fallback = t('Failed to render diagram.');
          setError(
            caughtError instanceof Error && caughtError.message
              ? caughtError.message
              : fallback,
          );
          setSvgMarkup('');
        }
      } finally {
        if (isActive) {
          setIsLoading(false);
        }
      }
    };

    renderDiagram();

    return () => {
      isActive = false;
      controller.abort();
    };
  }, [diagramSource, diagramType]);

  return (
    <Root width={width} height={height}>
      {isLoading && <Message>{t('Rendering diagram...')}</Message>}
      {!isLoading && error && <ErrorMessage>{error}</ErrorMessage>}
      {!isLoading && !error && (
        <div
          // SVG content is sanitized on the backend before returning.
          dangerouslySetInnerHTML={{ __html: svgMarkup }}
        />
      )}
    </Root>
  );
}
