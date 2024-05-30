set v_role = 'SYSADMIN';
set v_warehouse = 'COMPUTE_WH';
set v_database = 'SANDBOX_DB';
set v_schema = 'WORKPLACE_POLICY_DEMO';

-- set the session context
use role identifier($v_role);
use warehouse identifier($v_warehouse);
use database identifier($v_database);
use schema identifier($v_schema);


-- Display current session information to confirm you are operating in your desired env
select
    current_user(),
    current_warehouse(),
    current_role(),
    current_database(),
    current_schema();


-- Create a stage named workplace_policies with encryption and directory options
create stage if not exists workplace_policies
    encryption = (type = 'snowflake_sse')
    directory = ( enable = true )
    comment = 'Workplace policy documents';

-- Show all stages
show stages;

-- Describe the workplace_policies stage
desc stage WORKPLACE_POLICIES;

/*
Upload your pdf documents to this stage
*/

-- List the contents of the workplace_policies stage
ls @workplace_policies;

-- Create or replace a Python UDF named pdf_text_chunker
create or replace function pdf_text_chunker(file_url string)
returns table (chunk varchar)
language python
runtime_version = '3.9'
handler = 'pdf_text_chunker'
packages = ('snowflake-snowpark-python','PyPDF2', 'langchain')
as
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2, io
import logging
import pandas as pd

class pdf_text_chunker:

    def read_pdf(self, file_url: str) -> str:
        logger = logging.getLogger("udf_logger")
        logger.info(f"Opening file {file_url}")
    
        with SnowflakeFile.open(file_url, 'rb') as f:
            buffer = io.BytesIO(f.readall())
            
        reader = PyPDF2.PdfReader(buffer)   
        text = ""
        for page in reader.pages:
            try:
                text += page.extract_text().replace('\n', ' ').replace('\0', ' ')
            except:
                text = "Unable to Extract"
                logger.warn(f"Unable to extract from file {file_url}, page {page}")
        
        return text

    def process(self, file_url: str):
        text = self.read_pdf(file_url)
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 4000, # Adjust this as needed
            chunk_overlap  = 400, # Overlap helps keep chunks contextual
            length_function = len
        )
    
        chunks = text_splitter.split_text(text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        
        yield from df.itertuples(index=False, name=None)
$$;

-- Create or replace a table named DOCS_CHUNKS_TABLE to store PDF text chunks and their embeddings
create or replace TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped URL (choose based on use case)
    CHUNK VARCHAR(16777216), -- Piece of text
    CHUNK_VEC VECTOR(FLOAT, 768) -- Embedding using the VECTOR data type
);

-- Insert data into DOCS_CHUNKS_TABLE by processing files in the workplace_policies stage
insert into docs_chunks_table (relative_path, size, file_url,
                            scoped_file_url, chunk, chunk_vec)
select relative_path, 
            size,
            file_url, 
            build_scoped_file_url(@workplace_policies, relative_path) as scoped_file_url,
            func.chunk as chunk,
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m',chunk) as chunk_vec
    from 
        directory(@workplace_policies),
        TABLE(pdf_text_chunker(build_scoped_file_url(@workplace_policies, relative_path))) as func;

-- Select all data from DOCS_CHUNKS_TABLE
select * from docs_chunks_table;

-- example: Concatenate all chunks into a single text using listagg
select listagg(chunk, ' ') as concatenated_chunks from docs_chunks_table;


-- Create or replace a table named combined_policy to store the generated comprehensive policy document
create or replace table combined_policy
as
select 
snowflake.cortex.complete('mixtral-8x7b',
$$
You are tasked with merging multiple workplace violence policies into one comprehensive and cohesive policy document. The policies provided cover various aspects of workplace violence, including prevention, reporting procedures, response protocols, and employee support. Your goal is to integrate these policies seamlessly, ensuring thoroughness, clarity, consistency, and completeness. Follow these detailed guidelines:

1. Organize Logically: Structure the combined policy into clear, well-defined sections with appropriate headings. Each section should comprehensively cover a specific area (e.g., prevention, reporting procedures, response protocols, employee support). Ensure there is a logical flow between sections.

2. Maintain Consistency: Ensure that the terminology, tone, and style are consistent throughout the document. Use uniform language and definitions for similar concepts across different sections to maintain coherence.

3. Eliminate Redundancies: Identify and remove any redundant statements or sections. Consolidate similar policies to avoid repetition while preserving important details and nuances.

4. Enhance Clarity: Rewrite complex or ambiguous sentences to improve clarity. Use clear, concise language and provide examples where necessary to ensure each policy is easy to understand for all employees.

5. Ensure Compliance: Verify that the combined policy adheres to all relevant legal and regulatory requirements. Include references to applicable laws and regulations where necessary, and ensure the policy is up-to-date with current standards.

6. Include Definitions: Provide a comprehensive section for definitions of key terms used in the policy. This helps ensure that everyone has a clear understanding of important concepts and terminology.

7. Detail Procedures: Thoroughly detail all procedures for preventing, reporting, and responding to workplace violence. Ensure that each step is clearly outlined, including roles and responsibilities of employees and management.

8. Provide Resources and Support: Include information on available resources and support for employees affected by workplace violence. Detail any counseling services, hotlines, or support groups available.

9. Review and Revise: After combining the policies, meticulously review the entire document for coherence, completeness, and accuracy. Make any necessary revisions to improve the overall quality, ensuring that all sections are comprehensive and aligned with the overall policy objectives.

10. Add an Appendix: Consider adding an appendix for supplementary information such as contact lists, emergency procedures, and frequently asked questions (FAQs).

Below are the text excerpts from the different workplace violence policies:

$$ 

|| listagg(chunk,' ') || 

$$ Using the

 provided text excerpts, create a comprehensive workplace violence policy document that integrates all aspects as described above. Ensure the final document is thorough, detailed, and user-friendly.$$
) as model_response,
current_timestamp() as created_ts
from docs_chunks_table;

-- Select all data from combined_policy
select * from combined_policy;

-- Generate feedback on the combined policy using Snowflake Cortex
select
    snowflake.cortex.complete('mixtral-8x7b','How can the following workplace violence policy be improved? Policy: ' || model_response)
from combined_policy;

-- Generate quiz questions based on the combined policy using Snowflake Cortex
select
snowflake.cortex.complete(
'mixtral-8x7b',
$$
# IDENTITY and PURPOSE

You are an expert on the subject defined in the input section provided below.

# GOAL

Generate questions for a student who wants to review the main concepts of the learning objectives provided in the input section provided below.

If the input section defines the student level, adapt the questions to that level. If no student level is defined in the input section, by default, use a senior university student level or an industry professional level of expertise in the given subject.

Do not answer the questions.

Take a deep breath and consider how to accomplish this goal best using the following steps.

# STEPS

- Extract the subject of the input section.
- Redefine your expertise on that given subject.
- Extract the learning objectives of the input section.
- Generate, upmost, three review questions for each learning objective. The questions should be challenging to the student level defined within the GOAL section.

# OUTPUT INSTRUCTIONS

- Output in clear, human-readable Markdown.
- Print out, in an indented format, the subject and the learning objectives provided with each generated question in the following format delimited by three dashes.
Do not print the dashes. 
---
Subject: 
* Learning objective: 
    - Question 1: generated question 1
    - Answer 1: 

    - Question 2: generated question 2
    - Answer 2:
    
    - Question 3: generated question 3
    - Answer 3:
---


# INPUT:

INPUT: 
$$ ||
model_response) as policy_quiz
from combined_policy;
