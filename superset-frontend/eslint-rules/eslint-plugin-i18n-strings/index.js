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

/**
 * @fileoverview Rule to warn about translation template variables
 * @author Apache
 */

//------------------------------------------------------------------------------
// Rule Definition
//------------------------------------------------------------------------------

/** @type {import('eslint').Rule.RuleModule} */
module.exports = {
  rules: {
    'no-template-vars': {
      meta: {
        type: 'problem',
        docs: {
          description: 'Disallow variables in translation template strings',
        },
        schema: [],
      },
      create(context) {
        function handler(node) {
          for (const arg of node.arguments ?? []) {
            if (
              arg.type === 'TemplateLiteral' &&
              (arg.expressions?.length ?? 0) > 0
            ) {
              context.report({
                node,
                message:
                  "Don't use variables in translation string templates. Flask-babel is a static translation service, so it can't handle strings that include variables",
              });
              break;
            }
          }
        }
        return {
          "CallExpression[callee.name='t']": handler,
          "CallExpression[callee.name='tn']": handler,
        };
      },
    },
    'sentence-case-buttons': {
      meta: {
        type: 'suggestion',
        docs: {
          description: 'Enforce sentence case for button text in translations',
        },
        schema: [],
      },
      create(context) {
        function isTitleCase(str) {
          // Match "Delete Dataset", "Create Chart", etc. (2+ title-cased words)
          return /^[A-Z][a-z]+(\s+[A-Z][a-z]*)+$/.test(str);
        }

        function isButtonContext(node) {
          const parent = node.parent;
          if (!parent) return false;

          // Check for button-specific props
          if (parent.type === 'Property') {
            const key = parent.key?.name;
            return [
              'primaryButtonName',
              'secondaryButtonName',
              'confirmButtonText',
              'cancelButtonText',
            ].includes(key);
          }

          // Check for Button components
          if (String(parent.type) === 'JSXExpressionContainer') {
            const jsx = parent.parent;
            if (String(jsx?.type) === 'JSXElement') {
              const elementName = jsx?.openingElement?.name?.name;
              return elementName === 'Button';
            }
          }

          return false;
        }

        function handler(node) {
          for (const arg of node.arguments ?? []) {
            if (arg.type !== 'Literal' || typeof arg.value !== 'string') {
              continue;
            }
            const text = arg.value;

            if (isButtonContext(node) && isTitleCase(text)) {
              const sentenceCase = text
                .toLowerCase()
                .replace(/^\w/, c => c.toUpperCase());
              context.report({
                node: arg,
                message: `Button text should use sentence case: "${text}" should be "${sentenceCase}"`,
              });
            }
          }
        }

        return {
          "CallExpression[callee.name='t']": handler,
          "CallExpression[callee.name='tn']": handler,
        };
      },
    },
  },
};
