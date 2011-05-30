/*
 * Copyright 2006-2007 Luca Garulli (luca.garulli--at--assetdata.it)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.romaframework.aspect.persistence.datanucleus.jdo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.romaframework.core.resource.ResourceResolverListener;

/**
 * JDO Metadata listener to collect all .jdo files to be enhanced later by JDOBasePersistenceAspect.
 * 
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 */
public class JDOMetadataResourceResolver implements ResourceResolverListener {

  private static final String METADATA_EXT = ".jdo";

  private List<String>        metadatas    = new ArrayList<String>();

  public void addResource(File file, String name, String packagePrefix, String startingPackage) {
    if (!name.endsWith(METADATA_EXT))
      return;

    metadatas.add(packagePrefix + "/" + name);
  }

  public String[] getMetadatas() {
    return metadatas.toArray(new String[metadatas.size()]);
  }
}
