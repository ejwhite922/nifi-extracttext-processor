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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import com.dataflowdeveloper.processors.process.ExtractTextProcessor.ExtractTextAttributes;
import com.dataflowdeveloper.processors.process.ExtractTextProcessor.OutputMode;


public class ExtractTextProcessorTest {
	private static final String MIME_TYPE = CoreAttributes.MIME_TYPE.key();
	private static final String ORIG_MIME_TYPE = ExtractTextAttributes.ORIG_MIME_TYPE.key();

	private TestRunner testRunner;

	@Before
	public void init() {
		testRunner = TestRunners.newTestRunner(ExtractTextProcessor.class);
	}

	@Test
	public void processor_should_support_pdf_types_without_exception() throws Exception {
		final String filename = "simple.pdf";
		setupTestWithFile(filename);

		testRunner.assertValid();
		testRunner.run();
		testRunner.assertTransferCount(ExtractTextProcessor.REL_FAILURE, 0);


		final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractTextProcessor.REL_SUCCESS);
		for (final MockFlowFile mockFile : successFiles) {
			final String result = new String(mockFile.toByteArray(), StandardCharsets.UTF_8);
			final String trimmedResult = result.trim();
			assertTrue(trimmedResult.startsWith("A Simple PDF File"));
			System.out.println("FILE:" + result);
		}
	}

	@Test
	public void processor_should_support_doc_types_without_exception() throws Exception {
		final String filename = "simple.doc";
		setupTestWithFile(filename);

		testRunner.assertValid();
		testRunner.run();
		testRunner.assertTransferCount(ExtractTextProcessor.REL_FAILURE, 0);


		final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractTextProcessor.REL_SUCCESS);
		for (final MockFlowFile mockFile : successFiles) {
			final String result = new String(mockFile.toByteArray(), StandardCharsets.UTF_8);
			final String trimmedResult = result.trim();
			assertTrue(trimmedResult.startsWith("A Simple WORD DOC File"));
			System.out.println("FILE:" + result);
		}
	}

	@Test
	public void processor_should_support_docx_types_without_exception() throws Exception {
		final String filename = "simple.docx";
		setupTestWithFile(filename);

		testRunner.assertValid();
		testRunner.run();
		testRunner.assertTransferCount(ExtractTextProcessor.REL_FAILURE, 0);


		final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractTextProcessor.REL_SUCCESS);
		for (final MockFlowFile mockFile : successFiles) {
			final String result = new String(mockFile.toByteArray(), StandardCharsets.UTF_8);
			final String trimmedResult = result.trim();
			assertTrue(trimmedResult.startsWith("A Simple WORD DOCX File"));
			System.out.println("FILE:" + result);
		}
	}

	@Test
	public void when_running_processor_mime_type_should_be_discovered_for_pdf_input() throws Exception {
		final String filename = "simple.pdf";
		setupTestWithFile(filename);

		testRunner.assertValid();
		testRunner.run();

		testRunner.assertAllFlowFilesTransferred(ExtractTextProcessor.REL_SUCCESS);
		final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractTextProcessor.REL_SUCCESS);
		for (final MockFlowFile mockFile : successFiles) {
			mockFile.assertAttributeExists(MIME_TYPE);
			mockFile.assertAttributeEquals(MIME_TYPE, "text/plain");
			mockFile.assertAttributeExists(ORIG_MIME_TYPE);
			mockFile.assertAttributeEquals(ORIG_MIME_TYPE, "application/pdf");
		}
	}

	@Test
	public void when_running_processor_mime_type_should_be_discovered_for_pdf_input_html() throws Exception {
		final String filename = "simple.pdf";
		setupTestWithFile(filename);

		testRunner.setProperty(ExtractTextProcessor.OUTPUT_MODE, OutputMode.HTML.toString());

		testRunner.assertValid();
		testRunner.run();

		testRunner.assertAllFlowFilesTransferred(ExtractTextProcessor.REL_SUCCESS);
		final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractTextProcessor.REL_SUCCESS);
		for (final MockFlowFile mockFile : successFiles) {
			mockFile.assertAttributeExists(MIME_TYPE);
			mockFile.assertAttributeEquals(MIME_TYPE, "text/html");
			mockFile.assertAttributeExists(ORIG_MIME_TYPE);
			mockFile.assertAttributeEquals(ORIG_MIME_TYPE, "application/pdf");
		}
	}

	@Test
	public void when_running_processor_mime_type_should_be_discovered_for_doc_input() throws Exception {
		final String filename = "simple.doc";
		setupTestWithFile(filename);

		testRunner.assertValid();
		testRunner.run();

		testRunner.assertAllFlowFilesTransferred(ExtractTextProcessor.REL_SUCCESS);
		final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractTextProcessor.REL_SUCCESS);
		for (final MockFlowFile mockFile : successFiles) {
			mockFile.assertAttributeExists(MIME_TYPE);
			mockFile.assertAttributeEquals(MIME_TYPE, "text/plain");
			mockFile.assertAttributeExists(ORIG_MIME_TYPE);
			mockFile.assertAttributeEquals(ORIG_MIME_TYPE, "application/msword");
		}
	}

	@Test
	public void when_running_processor_mime_type_should_be_discovered_for_docx_input() throws Exception {
		final String filename = "simple.docx";
		setupTestWithFile(filename);

		testRunner.assertValid();
		testRunner.run();

		testRunner.assertAllFlowFilesTransferred(ExtractTextProcessor.REL_SUCCESS);
		final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractTextProcessor.REL_SUCCESS);
		for (final MockFlowFile mockFile : successFiles) {
			mockFile.assertAttributeExists(MIME_TYPE);
			mockFile.assertAttributeEquals(MIME_TYPE, "text/plain");
			mockFile.assertAttributeExists(ORIG_MIME_TYPE);
			mockFile.assertAttributeEquals(ORIG_MIME_TYPE, "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
		}
	}

	@Test
	public void when_running_processor_as_default_unlimited_text_length_should_be_used() throws Exception {
		final String filename = "big.pdf";
		setupTestWithFile(filename);

		testRunner.assertValid();
		testRunner.run();

		testRunner.assertAllFlowFilesTransferred(ExtractTextProcessor.REL_SUCCESS);
		final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractTextProcessor.REL_SUCCESS);
		for (final MockFlowFile mockFile : successFiles) {
			final String result = new String(mockFile.toByteArray(), StandardCharsets.UTF_8);
			assertTrue(result.length() > 100);
			System.out.println(Integer.toString(result.length()));
			System.out.println("FILE:" + result);
		}
	}

	@Test
	public void when_running_processor_with_limit_text_length_should_be_less_than_or_equal_to_limit() throws Exception {
		final String filename = "simple.pdf";
		setupTestWithFile(filename);

		testRunner.setProperty(ExtractTextProcessor.MAX_TEXT_LENGTH, "100");
		testRunner.assertValid();
		testRunner.run();

		testRunner.assertAllFlowFilesTransferred(ExtractTextProcessor.REL_SUCCESS);
		final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExtractTextProcessor.REL_SUCCESS);
		for (final MockFlowFile mockFile : successFiles) {
			final String result = new String(mockFile.toByteArray(), StandardCharsets.UTF_8);
			assertFalse(result.length() > 100);
			System.out.println("FILE:" + result);
		}
	}

	private void setupTestWithFile(final String filename) throws Exception {
		final InputStream content = getClass().getResourceAsStream("/" + filename);
		final Map<String, String> attrs = new HashMap<>();
		attrs.put(CoreAttributes.FILENAME.key(), filename);
		testRunner.enqueue(content, attrs);
	}
}