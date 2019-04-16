/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataflowdeveloper.processors.process;

import static java.util.Objects.requireNonNull;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FlowFileAttributeKey;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.ExpandedTitleContentHandler;
import org.xml.sax.SAXException;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "extracttextprocessortika" })
@CapabilityDescription("Run Apache Tika Text Extraction from PDF, Word, Excel.   Parameter for HTML or TEXT output.  Parameter for Maximum Length returned.")
@WritesAttributes({ @WritesAttribute(attribute = "orig.mime.type", description = "The original MIME type of the file before Tika text extraction converted it to HTML or plain TEXT.") })
public class ExtractTextProcessor extends AbstractProcessor {
	public static final PropertyDescriptor MAX_TEXT_LENGTH = new PropertyDescriptor.Builder()
			.name("MAX_TEXT_LENGTH")
			.displayName("Max Output Text Length")
			.description(
					"The maximum length of text to retrieve. This is used to limit memory usage for dealing with large files. Specify -1 for unlimited length.")
			.required(false)
			.defaultValue("-1")
			.addValidator(StandardValidators.INTEGER_VALIDATOR)
			.build();

	public static final PropertyDescriptor OUTPUT_MODE = new PropertyDescriptor.Builder()
			.name("OUTPUT_MODE")
			.displayName("File Output Mode")
			.description("Set to html for HTML output or text for Text output")
			.required(false)
			.allowableValues(Stream.of(OutputMode.values()).map(OutputMode::getName).collect(Collectors.toSet()))
			.defaultValue(OutputMode.TEXT.toString())
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("Successfully extracted content.").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("Failed to extract content.").build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(MAX_TEXT_LENGTH);
		descriptors.add(OUTPUT_MODE);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		final int maxTextLength = context.getProperty(MAX_TEXT_LENGTH).asInteger();
		final String outputModeValue = context.getProperty(OUTPUT_MODE).getValue();
		final OutputMode outputMode = OutputMode.fromName(outputModeValue);
		final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());

		try {
			final AtomicReference<String> type = new AtomicReference<>();

			flowFile = session.write(flowFile, new StreamCallback() {
				@Override
				public void process(final InputStream inputStream, final OutputStream outputStream) throws IOException {
					try (final BufferedInputStream buffStream = new BufferedInputStream(inputStream)) {
						final Tika tika = new Tika();
						String text = "";
						try {
							type.set(tika.detect(buffStream, filename));

							switch (outputMode) {
							case HTML:
								// http://lifeinide.com/post/2013-10-18-convert-document-to-html-with-apache-tika/

								final ByteArrayOutputStream out = new ByteArrayOutputStream();
								final SAXTransformerFactory factory = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
								final TransformerHandler handler = factory.newTransformerHandler();
								handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "html");
								handler.getTransformer().setOutputProperty(OutputKeys.INDENT, "yes");
								handler.getTransformer().setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_8.toString());
								handler.setResult(new StreamResult(out));
								final ExpandedTitleContentHandler handler1 = new ExpandedTitleContentHandler(handler);

								final AutoDetectParser parser = new AutoDetectParser();
								parser.parse(buffStream, handler1, new Metadata());
								text = handler1.toString();
								break;
							case TEXT:
								tika.setMaxStringLength(maxTextLength);
								text = tika.parseToString(buffStream);
								break;
							default:
								throw new ProcessException("Invalid output mode: " + outputModeValue);
							}
						} catch (final TikaException e) {
							throw new ProcessException("Apache Tika failed to parse input ", e);
						} catch (final SAXException e) {
							throw new ProcessException(
									"Apache Tika failed to parse input on XML/HTML error ", e);
						} catch (final TransformerConfigurationException e) {
							throw new ProcessException(
									"Apache Tika failed to parse input on XML/HTML error ", e);
						}

						outputStream.write(text.getBytes());
					}
				}
			});

			final Map<String, String> mimeAttrs = new HashMap<>();
			mimeAttrs.put(ExtractTextAttributes.ORIG_MIME_TYPE.key(), type.get());
			mimeAttrs.put(CoreAttributes.MIME_TYPE.key(), outputMode.getMimeType());
			mimeAttrs.put(CoreAttributes.FILENAME.key(), updateFilename(filename, outputMode.getExtension()));

			flowFile = session.putAllAttributes(flowFile, mimeAttrs);
			session.transfer(flowFile, REL_SUCCESS);
		} catch (final Throwable t) {
			getLogger().error("Unable to process ExtractTextProcessor file", t);
			session.transfer(flowFile, REL_FAILURE);
		}
	}

	private static String updateFilename(final String filename, final String newExtension) {
		if (!filename.endsWith(newExtension)) {
			return filename + newExtension;
		}
		return filename;
	}

	/**
	 * The supported output modes.
	 */
	public static enum OutputMode {
		HTML("html", "text/html", ".html"),
		TEXT("text", "text/plain", ".txt");

		private final String name;
		private final String mimeType;
		private final String extension;

		/**
		 * Creates a new {@link OutputMode}
		 * @param name the name used to identify the output mode. (not null)
		 * @param mimeType the MIME type associated with the output mode. (not null)
		 * @param extension the file extension used by the output mode. (not null)
		 */
		private OutputMode(final String name, final String mimeType, final String extension) {
			this.name = requireNonNull(name);
			this.mimeType = requireNonNull(mimeType);
			this.extension = requireNonNull(extension);
		}

		/**
		 * @return the name used to identify the output mode. (not null)
		 */
		public String getName() {
			return name;
		}

		/**
		 * @return the MIME type associated with the output mode.
		 */
		public String getMimeType() {
			return mimeType;
		}

		/**
		 * @return the file extension used by the output mode.
		 */
		public String getExtension() {
			return extension;
		}

		@Override
		public String toString() {
			return getName();
		}

		/**
		 * Finds the output mode by name.
		 * @param name the name to find.
		 * @return the {@link OutputMode} or {@code null} if none could be found.
		 */
		public static OutputMode fromName(final String name) {
			for (final OutputMode outputMode : OutputMode.values()) {
				if (outputMode.getName().equalsIgnoreCase(name)) {
					return outputMode;
				}
			}
			return null;
		}
	}

	/**
	 * FlowFile attributes produced by {@link ExtractTextProcessor}.
	 */
	public static enum ExtractTextAttributes implements FlowFileAttributeKey {
		/**
		 * The original MIME type of the file before Tika text extraction
		 * converted it to HTML or plain TEXT.
		 */
		ORIG_MIME_TYPE("orig.mime.type");

		private final String key;

		/**
		 * Creates a new {@link ExtractTextAttributes}.
		 * @param key the attribute's key name. (not null)
		 */
		private ExtractTextAttributes(final String key) {
			this.key = requireNonNull(key);
		}

		@Override
		public String key() {
			return key;
		}
	}
}
