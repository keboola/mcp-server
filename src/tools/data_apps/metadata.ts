import { MetadataField } from '@/constants';
import { logger } from '@/logger';
import type { StorageHelpers } from './client';
import { CREATED_BY_MCP, type MetadataItem, UPDATED_BY_MCP_PREFIX } from './utils';

// --- metadata helpers (ports of components/utils.py) ------------------------

export const setCfgCreationMetadata = async (
  helpers: StorageHelpers,
  configurationId: string,
): Promise<void> => {
  try {
    await helpers.configurationMetadataUpdate(configurationId, { [CREATED_BY_MCP]: 'true' });
  } catch (error) {
    logger.error(
      { err: error },
      `Failed to set "${CREATED_BY_MCP}" metadata for ${configurationId}`,
    );
  }
};

export const setCfgUpdateMetadata = async (
  helpers: StorageHelpers,
  configurationId: string,
  configurationVersion: number,
): Promise<void> => {
  const key = `${UPDATED_BY_MCP_PREFIX}${configurationVersion}`;
  try {
    await helpers.configurationMetadataUpdate(configurationId, { [key]: 'true' });
  } catch (error) {
    logger.error({ err: error }, `Failed to set "${key}" metadata for ${configurationId}`);
  }
};

const buildFolderHint = (
  total: number,
  existingFolders: string[],
  configLabel: string,
  updateTool: string,
  lowerBound: boolean,
): string | null => {
  if (total < 20) return null;
  const countStr = lowerBound ? `at least ${total}` : String(total);
  let hint = `Note: This project already has ${countStr} ${configLabel}. Consider organizing them with folders. `;
  if (existingFolders.length > 0) {
    hint +=
      `Existing folders: ${existingFolders.join(', ')}. ` +
      `Call ${updateTool} with a folder= parameter to assign this to one.`;
  } else {
    hint += `No folders have been created yet. Call ${updateTool} with a folder= parameter to start organizing.`;
  }
  return hint;
};

const getConfigFolders = async (
  helpers: StorageHelpers,
): Promise<{ total: number; folders: string[]; lowerBound: boolean }> => {
  const allConfigs = await helpers.configurationList();
  const seen = new Set<string>();
  const folders: string[] = [];
  let folderBearing = 0;
  for (const cfg of allConfigs) {
    const metadata = (cfg.metadata as MetadataItem[]) ?? [];
    let hasFolder = false;
    for (const meta of metadata) {
      if (meta.key === MetadataField.CONFIGURATION_FOLDER_NAME) {
        hasFolder = true;
        const folderName = (meta.value ?? '').trim();
        if (folderName && !seen.has(folderName)) {
          seen.add(folderName);
          folders.push(folderName);
        }
      }
    }
    if (hasFolder) folderBearing += 1;
  }
  // configuration_list does not embed metadata server-side the way the search endpoint does,
  // so we derive the total from the same list (faithful to the resulting hint behavior).
  const total = allConfigs.length;
  if (folderBearing >= 20) return { total: folderBearing, folders, lowerBound: true };
  if (total < 20) return { total, folders: [], lowerBound: false };
  return { total, folders, lowerBound: false };
};

export const applyFolderMetadata = async (
  helpers: StorageHelpers,
  configurationId: string,
  folder: string | null | undefined,
  plural: string,
  toolName: string,
  isNew = false,
): Promise<string | null> => {
  if (folder == null) {
    try {
      const { total, folders, lowerBound } = await getConfigFolders(helpers);
      return buildFolderHint(total, folders, plural, toolName, lowerBound);
    } catch {
      logger.warn(`Unable to fetch ${plural} folders for configuration "${configurationId}".`);
      return null;
    }
  }
  const normalized = folder.trim();
  if (normalized) {
    try {
      await helpers.configurationMetadataUpdate(configurationId, {
        [MetadataField.CONFIGURATION_FOLDER_NAME]: normalized,
      });
    } catch {
      logger.warn(`Unable to set folder metadata for configuration "${configurationId}".`);
    }
  } else if (!isNew) {
    try {
      const metadata = await helpers.configurationMetadataGet(configurationId);
      for (const entry of metadata) {
        if (entry.key === MetadataField.CONFIGURATION_FOLDER_NAME && entry.id) {
          await helpers.configurationMetadataDelete(configurationId, entry.id);
        }
      }
    } catch {
      logger.warn(`Unable to clear folder metadata for configuration "${configurationId}".`);
    }
  }
  return null;
};
