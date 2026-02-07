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
import {
  ControlPanelConfig,
  formatSelectOptions,
} from '@superset-ui/chart-controls';
import { t } from '@apache-superset/core';
import { validateNonEmpty } from '@superset-ui/core';

const diagramTypes = formatSelectOptions([
  'mermaid',
  'plantuml',
  'graphviz',
  'd2',
  'bpmn',
  'nomnoml',
  'svgbob',
  'vega',
  'vegalite',
]);

const controlPanel: ControlPanelConfig = {
  controlPanelSections: [
    {
      label: t('Diagram'),
      expanded: true,
      controlSetRows: [
        [
          {
            name: 'diagram_type',
            config: {
              type: 'SelectControl',
              label: t('Diagram type'),
              description: t('Kroki renderer used for SVG output.'),
              clearable: false,
              default: 'mermaid',
              choices: diagramTypes,
              renderTrigger: true,
            },
          },
        ],
        [
          {
            name: 'diagram_source',
            config: {
              type: 'TextAreaControl',
              language: 'markdown',
              label: t('Diagram source'),
              description: t(
                'Diagram source sent to the Kroki sidecar and rendered as SVG.',
              ),
              default:
                'graph TD\n  Client[Client] --> Superset[Superset]\n  Superset --> Kroki[Kroki Sidecar]',
              rows: 16,
              renderTrigger: true,
              validators: [validateNonEmpty],
            },
          },
        ],
      ],
    },
  ],
};

export default controlPanel;
